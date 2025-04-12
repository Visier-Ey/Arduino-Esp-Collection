#include "HardwareSerial.h"
#include "VMQTT.h"
#include <cstdint>

#define DEBUG

#define BUFFER_SIZE 1024*8
uint8_t serialBuffer[BUFFER_SIZE];
uint16_t bufferIndex = 0;


#define CMD_HEADER 0x05
#define CMD_END 0xfe
#define CMD_Coord 0x02
#define CMD_Freq 0x03

void publishCoord(uint16_t* payload, uint16_t length);
void parseCommand(uint8_t* data, uint16_t length);
void publishFreq(uint16_t freq);

void handleSerial() {
    while(Serial.available() > 0) {
        static bool end_count = false;
        if(bufferIndex >= BUFFER_SIZE-1) { // 留一个字节空间
            Serial.println("Error: Serial buffer overflow!");
            bufferIndex = 0;
            return;
        }
        serialBuffer[bufferIndex++] = Serial.read();
        if (serialBuffer[0]!=CMD_HEADER) bufferIndex=0;
        // transimit end
        if(serialBuffer[bufferIndex-1] == CMD_END) {
          if (end_count)
            {
              parseCommand(serialBuffer, bufferIndex-2); // remove '\n'
              bufferIndex = 0;
              end_count = false;
            }else{
              end_count = true;
            }
        }
    }
}



void parseCommand(uint8_t* data, uint16_t length) {
    if(length < 2 || (length-2)%2 != 0) { // 确保长度有效
      Serial.println("Invalid data length");
      return;
    }
    
    uint16_t cmd = (data[0] << 8) | data[1];
    
    if((cmd >> 8) != CMD_HEADER) {
        Serial.println("Error! Invaild Header!");
        Serial.println(cmd>>8);
        bufferIndex=0;
        return;
    }
    
    uint8_t cmdType = cmd & 0xFF;
    uint16_t* payload = (uint16_t*)(data + 2);
    uint8_t payloadLength = (length - 2) / 2;
    if (payloadLength%2 != 0){
      Serial.println("Data Length Error!");
      Serial.println(payloadLength);
      return;
    }else {
      Serial.println("Send Data Length");
      Serial.println(payloadLength);
    }
    #ifdef DEBUG
      Serial.println("SendMessage");
    #endif
    switch(cmdType) {
        case CMD_Coord:
            publishCoord(payload,payloadLength);
            break;
            
        case CMD_Freq:
            publishFreq(*payload);
            break;
            
        default:
            Serial.println("Unknown Cmd Type");
            break;
    }
}


// 全局变量定义
StaticJsonDocument<512*2> coordDoc;  // 内存减半
char coordJsonBuffer[512*2];        // 缓冲区减半

void publishCoord(uint16_t* payload, uint16_t length) {
  // 清空文档
  vmqtt.update();
  coordDoc.clear();
  
  // 使用纯数组格式
  JsonObject data = coordDoc.to<JsonObject>();
  data["device"] = "ESP8266_Node";  // 设备名称
  data["timestamp"] = millis();     // 时间戳
  
  JsonArray coords = data.createNestedArray("coordinates");
  for(int i = 0; i < length; i += 2) {
    coords.add(payload[i]);    // x坐标
    coords.add(payload[i+1]);  // y坐标
  }

  // 序列化
  size_t len = serializeJson(coordDoc, coordJsonBuffer);
  if (len == 0 || len >= sizeof(coordJsonBuffer)) {
    Serial.printf("[JSON] Serialize failed or overflow (%d/%d)\n", 
                len, sizeof(coordJsonBuffer));
    return;
  }

  vmqtt.update();
  // 调试输出
  

  // 发布
  if(!vmqtt.publish(coordJsonBuffer, topic_xy)) {
    Serial.println("MQTT publish failed");
  }else {
    #ifdef DEBUG
      Serial.printf("JSON size: %d bytes, %d points\n", len, coords.size());
      Serial.println(coordJsonBuffer); // 打印实际JSON内容
    #endif
  }
}


void publishFreq(uint16_t freq) {
  
  // freqDoc["device"] = "ESP8266_Node";
  // freqDoc["timestamp"] = millis();
  // freqDoc["frequency"] = freq;  

  // serializeJson(freqDoc, freqBuffer);
  // if(!vmqtt.publish(freqBuffer, topic_freq)) {  
  //   Serial.println("MQTT freq publish failed");
  // }

  // #ifdef DEBUG
  // Serial.printf("public frequency：%dHz\n", freq);
  // #endif
}








