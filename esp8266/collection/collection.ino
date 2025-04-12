#include <ESP8266WiFi.h>
#include <ArduinoJson.h>
#include "VMQTT.h"  // 修改为使用VMQTTClient头文件
#include "wifi.h"
#include "serial.h"

#define DEBUG

void setup() {
  Serial.begin(115200);
  delay(1000);
  
  pinMode(LED_BUILTIN, OUTPUT);
  digitalWrite(LED_BUILTIN, HIGH);
  
  setupWifi();

  setupVMQTT();
}

void loop() {
  WiFiLoop();
  // 串口数据处理（保持不变）
  handleSerial();
  VMQTTLoop();
  // 定期发布示例数据（修改发布方式）
  static unsigned long lastPublishTime = 0;
  if (millis() - lastPublishTime > 5000) {
    lastPublishTime = millis();
    // publishExampleData();
  }

  delay(100);
}

