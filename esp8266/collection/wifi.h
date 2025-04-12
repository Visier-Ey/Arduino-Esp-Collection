
#include <ESP8266WiFi.h>
const char* ssid = "haha";         // Replace with your
const char* password = "66666666";  // WiFi credentials
int wifiTimeOutCounter = 0;

WiFiClient wifiClient;  // 新增网络客户端实例

void setupWifi(){
  pinMode(LED_BUILTIN, OUTPUT);
  Serial.print("Connecting to ");
  Serial.print(ssid);
  Serial.print(" ");
  WiFi.begin(ssid, password);
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
    digitalWrite(LED_BUILTIN, !digitalRead(LED_BUILTIN));
  }

  // wifiClient.setBufferSizes(2048, 512);
  digitalWrite(LED_BUILTIN, HIGH);
  Serial.println("");
  Serial.println("WiFi connected.");
  Serial.print("IP address: ");
  Serial.println(WiFi.localIP());
}

void handleWiFiDisconnect() {
  Serial.println("Reconnect WiFi...");
  WiFi.reconnect();
  
  while (WiFi.status() != WL_CONNECTED) {
    delay(500);
    Serial.print(".");
    digitalWrite(LED_BUILTIN, !digitalRead(LED_BUILTIN));
  }
  // wifiClient.setBufferSizes(1024, 1024);  // 接收 2048，发送 512
  
  Serial.println("Reconnect successfully");
  Serial.print("IP Address: ");
  Serial.println(WiFi.localIP());
  digitalWrite(LED_BUILTIN, LOW);
}

void WiFiLoop(){
  if (WiFi.status() != WL_CONNECTED) {
    handleWiFiDisconnect();
  }
}