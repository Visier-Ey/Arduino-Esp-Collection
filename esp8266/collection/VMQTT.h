#ifndef __VMQTT__
#define __VMQTT__

#define MQTT_MAX_TRANSFER_SIZE 256
#define CHUNK_SIZE 150  // 每个数据块的最大字节数
#define MAX_RETRIES 3   // 单个数据块的最大重试次数
#include <PubSubClient.h>
extern WiFiClient wifiClient; 

// 主题配置（保持不变）
const char* topic_led = "home/led/control";
const char* topic_config = "home/config";
const char* topic_xy = "device/xy-coords";
const char* topic_freq = "device/frequency"; 


class VMQTT {
private:
    PubSubClient mqttClient;
    Client* networkClient;  // 抽象网络客户端
    
    // 配置参数
    const char* mqttServer;
    uint16_t mqttPort;
    const char* mqttUser;
    const char* mqttPassword;
    const char* defaultTopic;
    
    // 状态管理
    unsigned long lastReconnectAttempt = 0;
    const unsigned long reconnectInterval = 5000;

public:
    // /​**​
    //  * 构造函数（依赖注入网络客户端）
    //  * @param client 实现Client接口的网络对象（WiFiClient/EthernetClient等）
    //  * @param server MQTT服务器地址
    //  * @param port   端口号（默认1883）
    //  * @param user   用户名（可选）
    //  * @param pass   密码（可选）
    //  * @param topic  默认主题（可选）
    //  */
    VMQTT(Client& client, 
               const char* server = "broker.emqx.io", 
               uint16_t port = 1883,
               const char* user = nullptr,
               const char* pass = nullptr,
               const char* topic = "default/topic")
        : networkClient(&client),
          mqttServer(server),
          mqttPort(port),
          mqttUser(user),
          mqttPassword(pass),
          defaultTopic(topic) {
        mqttClient.setClient(*networkClient);
        mqttClient.setBufferSize(1024);
        mqttClient.setKeepAlive(60);
    }

    // 初始化MQTT（非阻塞式）
    void begin() {
        mqttClient.setServer(mqttServer, mqttPort);
        mqttClient.setCallback([this](char* t, byte* p, unsigned int l) {
            this->handleMessage(t, p, l);
        });
        Serial.println("[MQTT] Client initialized");
    }

    // 维持连接（需在loop中调用）
    void update() {
        if (!mqttClient.connected()) {
            attemptReconnect();
        }
        mqttClient.loop();
    }

    // 发布消息（支持QoS和retain）
     bool publish(const String &payload, const char* topic = nullptr, bool retained = false) {
        if (payload.length() >= CHUNK_SIZE) {
            // 小数据直接发送
            return mqttClient.publish(topic ? topic : defaultTopic, payload.c_str(), retained);
        } else {
            // 大数据走分块逻辑
            return publishLargeData(payload, topic);
        }
    }

     /**
     * 分块发布大数据（核心改进）
     * @param payload 要发送的原始数据
     * @param topic 目标主题
     * @param isBinary 是否为二进制数据（默认false）
     */
    bool publishLargeData(const String &payload, const char* topic = nullptr, bool isBinary = false) {
        const char* targetTopic = topic ? topic : defaultTopic;
        const uint8_t* data = (const uint8_t*)payload.c_str();
        size_t totalLength = isBinary ? payload.length() : strlen(payload.c_str());
        
        // 发送元数据（总大小和分块数）
        int chunkCount = (totalLength + CHUNK_SIZE - 1) / CHUNK_SIZE;
      
        String metadata =  "{\"totalSize\":" + String(totalLength) + ",\"totalChunks\":" + String(chunkCount) + "}";
        if (!mqttClient.publish((String(targetTopic) + "/meta").c_str(), metadata.c_str())) {
            Serial.println("[MQTT] Failed to send metadata");
            return false;
        }

        // 分块发送
        for (int i = 0; i < chunkCount; i++) {
            size_t offset = i * CHUNK_SIZE;
            size_t chunkLen = min(CHUNK_SIZE, (int)(totalLength - offset));
            
            // 构造分块头
            String chunkHeader = "CHUNK:" + String(i) + "/" + String(chunkCount) + ":";
            
            // 发送分块（带重试机制）
            int retry = 0;
            bool chunkSuccess = false;
            while (retry < MAX_RETRIES && !chunkSuccess) {
                if (isBinary) {
                    // 二进制分块发送
                    chunkSuccess = mqttClient.publish_P(
                        (String(targetTopic) + "/chunk").c_str(),
                        data + offset,
                        chunkLen,
                        false
                    );
                } else {
                    // 文本分块发送
                    String chunkData = "{\"index\":" + String(i) + 
                                      ",\"total\":" + String(chunkCount) + 
                                      ",\"data\":\"" + escapeJsonString(payload.substring(offset, offset + chunkLen)) + "\"}"; 
                    chunkSuccess = mqttClient.publish(
                        (String(targetTopic) + "/chunk").c_str(),
                        chunkData.c_str()
                    );
                }
                
                if (!chunkSuccess) {
                    Serial.printf("[MQTT] Chunk %d failed (attempt %d)\n", i, retry + 1);
                    delay(100 * (retry + 1));  // 指数退避
                    retry++;
                }
            }

            if (!chunkSuccess) {
                Serial.printf("[MQTT] Failed to send chunk %d after %d retries\n", i, MAX_RETRIES);
                return false;
            }

            delay(10);  // 防止WiFi堵塞
        }

        // 发送结束标记
        mqttClient.publish((String(targetTopic) + "/end").c_str(), "TRANSMISSION_COMPLETE");
        return true;
    }

    // 订阅主题（支持通配符）
    void subscribe(const char* topic, uint8_t qos = 0) {
        if (mqttClient.subscribe(topic, qos)) {
            Serial.printf("[MQTT] Subscribed to %s\n", topic);
        }
    }

    // 设置消息回调（替代默认处理）
    void setMessageHandler(void (*callback)(const String&, const String&)) {
        messageCallback = callback;
    }

private:
    String escapeJsonString(const String &input) {
        String output;
        output.reserve(input.length() * 2); // 预留足够空间
        
        for (size_t i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            switch (c) {
                case '"':  output += "\\\""; break;
                case '\\': output += "\\\\"; break;
                case '\b': output += "\\b"; break;
                case '\f': output += "\\f"; break;
                case '\n': output += "\\n"; break;
                case '\r': output += "\\r"; break;
                case '\t': output += "\\t"; break;
                default:
                    if (c <= '\x1f') {
                        // 控制字符转Unicode转义
                        output += "\\u";
                        char buf[5];
                        snprintf(buf, sizeof(buf), "%04x", c);
                        output += buf;
                    } else {
                        output += c;
                    }
            }
        }
        return output;
    }

    void (*messageCallback)(const String& topic, const String& payload) = nullptr;

    // 消息处理
    void handleMessage(char* topic, byte* payload, unsigned int length) {
        String topicStr(topic);
        String payloadStr;
        payloadStr.reserve(length);
        
        for (unsigned int i = 0; i < length; i++) {
            payloadStr += (char)payload[i];
        }

        if (messageCallback) {
            messageCallback(topicStr, payloadStr);
        } else {
            defaultHandler(topicStr, payloadStr);
        }
    }

    // 默认消息处理
    void defaultHandler(const String& topic, const String& payload) {
        Serial.printf("[MQTT] Received [%s]: %s\n", 
                     topic.c_str(), payload.c_str());
    }

    // 重连逻辑
   // Add these debug checks to your attemptReconnect() function
    void attemptReconnect() {
        if (millis() - lastReconnectAttempt < reconnectInterval) return;
        
        lastReconnectAttempt = millis();
        
        // Generate more unique client ID with WiFi MAC address
        String clientId = "ESP-" + String(WiFi.macAddress());
        clientId.replace(":", ""); // Remove colons from MAC address
        
        Serial.printf("[MQTT] Attempting to connect as %s...\n", clientId.c_str());

        bool connected = false;
        if (mqttUser && mqttPassword) {
            connected = mqttClient.connect(
                clientId.c_str(), 
                mqttUser, 
                mqttPassword
            );
        } else {
            connected = mqttClient.connect(clientId.c_str());
        }

        if (connected) {
            Serial.println("[MQTT] Connected to broker");
            // Resubscribe to all topics
            subscribe(topic_led);
            subscribe(topic_config);
            subscribe(topic_xy);
            subscribe(topic_freq);
        } else {
            Serial.printf("[MQTT] Connect failed (rc=%d)\n", mqttClient.state());
            printMQTTError(mqttClient.state());
        }
    }

    void printMQTTError(int state) {
        switch(state) {
            case -4: Serial.println("Connection timeout"); break;
            case -2: Serial.println("Connection failed"); break;
            case -1: Serial.println("Client disconnected"); break;
            case 1: Serial.println("Unacceptable protocol version"); break;
            case 2: Serial.println("Identifier rejected"); break;
            case 3: Serial.println("Server unavailable"); break;
            case 4: Serial.println("Bad credentials"); break;
            case 5: Serial.println("Not authorized"); break;
            default: Serial.println("Unknown error"); break;
        }
    }
};

// const char* mqtt_server = "192.168.205.117";
const char* mqtt_server = "192.168.205.106";
const int mqtt_port = 1883;

VMQTT vmqtt(wifiClient, mqtt_server, mqtt_port);  



void setupVMQTT(){
  
  vmqtt.begin();
  vmqtt.subscribe(topic_led);
  vmqtt.setMessageHandler([](const String& topic, const String& payload) {
    Serial.print("Receive Message [");
    Serial.print(topic);
    Serial.print("]: ");
    Serial.println(payload);
    if (topic == topic_led) {
      if (payload == "on") {
        digitalWrite(LED_BUILTIN, LOW);
      } 
      else if (payload == "off") {
        digitalWrite(LED_BUILTIN, HIGH);
      }
    }
  });

}

void VMQTTLoop(){
  // MQTT连接管理（修改为单行调用）
  vmqtt.update();
}


#endif