const aedes = require('aedes')({
  mqttPacketStream: {
    highWaterMark: 1024 * 1024, // 1MB 缓冲区
  },
  concurrency: 100 // 提高并发处理能力
});
const server = require('net').createServer(aedes.handle);
const os = require('os');
const express = require('express');
const http = require('http');
const WebSocket = require('ws');

const MQTT_PORT = 1883;
const WEB_PORT = 3000;
const HOST = '0.0.0.0';

// Create Express app and HTTP server
const app = express();
const httpServer = http.createServer(app);

// Create WebSocket server
const wss = new WebSocket.Server({ server: httpServer });

// Store all connected WebSocket clients
const webClients = new Set();

// Serve static files
app.use(express.static('public'));

// WebSocket connection handler
wss.on('connection', (ws) => {
  webClients.add(ws);
  console.log('[WebSocket] New client connected');
  
  ws.on('close', () => {
    webClients.delete(ws);
    console.log('[WebSocket] Client disconnected');
  });
});

// Broadcast data to all WebSocket clients
function broadcast(data) {
  const message = JSON.stringify(data);
  webClients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(message);
    }
  });
}

// Start MQTT server
server.listen(MQTT_PORT, HOST, () => {
  console.log(`[MQTT] Server running on mqtt://${HOST}:${MQTT_PORT}`);
  console.log(`[MQTT] Local network IP: mqtt://${getLocalIP()}:${MQTT_PORT}`);
});

// Start web server
httpServer.listen(WEB_PORT, () => {
  console.log(`[Web] Server running on http://${HOST}:${WEB_PORT}`);
  console.log(`[Web] Local network IP: http://${getLocalIP()}:${WEB_PORT}`);
});

// MQTT client connection event
aedes.on('client', (client) => {
  console.log(`[MQTT] Client connected: ${client.id}`);
  broadcast({ 
    type: 'client', 
    id: client.id, 
    status: 'connected',
    timestamp: new Date().toISOString()
  });
});

// MQTT client disconnection event
aedes.on('clientDisconnect', (client) => {
  console.log(`[MQTT] Client disconnected: ${client.id}`);
  broadcast({ 
    type: 'client', 
    id: client.id, 
    status: 'disconnected',
    timestamp: new Date().toISOString()
  });
});

const chunkStorage = new Map(); // 格式: { clientId: { topic: { metadata, chunks, timer } } }

// 新增：分块超时清理（5分钟）
const CHUNK_TIMEOUT = 5 * 60 * 1000; 

// 处理分块数据的函数
function handleChunkedData(client, packet) {
  const topic = packet.topic;
  const payload = packet.payload.toString();
  const clientId = client.id;

  // 元数据消息处理
  if (topic.endsWith('/meta')) {
    try {
      console.log(payload)
      const meta = JSON.parse(payload);
      if (!meta.totalSize || !meta.totalChunks) {
        throw new Error('Invalid metadata format');
      }

      // 初始化存储结构
      if (!chunkStorage.has(clientId)) {
        chunkStorage.set(clientId, new Map());
      }
      
      const clientData = chunkStorage.get(clientId);
      clientData.set(topic.replace('/meta', ''), {
        metadata: meta,
        chunks: new Array(meta.totalChunks),
        received: 0,
        lastUpdated: Date.now(),
        timer: setTimeout(() => {
          console.log(`[Chunk] Timeout cleared incomplete transmission: ${clientId} ${topic}`);
          clientData.delete(topic.replace('/meta', ''));
        }, CHUNK_TIMEOUT)
      });

      console.log(`[Chunk] Metadata received for ${topic}: ${JSON.stringify(meta)}`);
    } catch (error) {
      console.error(`[Chunk] Metadata parse error from ${clientId}: ${error.message}`);
    }
    return;
  }

  // 分块数据处理
  if (topic.endsWith('/chunk')) {
    const baseTopic = topic.replace('/chunk', '');
    const clientData = chunkStorage.get(clientId);
    if (!clientData || !clientData.has(baseTopic)) {
      console.log(`[Chunk] Received chunk without metadata from ${clientId}`);
      return;
    }

    const transmission = clientData.get(baseTopic);
    clearTimeout(transmission.timer); // 重置超时计时器
    transmission.timer = setTimeout(() => {
      console.log(`[Chunk] Timeout cleared incomplete transmission: ${clientId} ${baseTopic}`);
      clientData.delete(baseTopic);
    }, CHUNK_TIMEOUT);

    try {
      const chunkData = JSON.parse(payload);
      // ========================================================================
      // if (!chunkData.index || !chunkData.total || !chunkData.data) {
      //   throw new Error('Invalid chunk format');
      // }

      // 检查分块索引有效性
      if (chunkData.index >= transmission.metadata.totalChunks) {
        throw new Error(`Chunk index ${chunkData.index} out of bounds`);
      }

      // 存储分块数据
      if (!transmission.chunks[chunkData.index]) {
        transmission.chunks[chunkData.index] = chunkData.data;
        transmission.received++;
        transmission.lastUpdated = Date.now();
      }

      console.log(`[Chunk] Received chunk ${chunkData.index + 1}/${transmission.metadata.totalChunks} from ${clientId} (${baseTopic})`);

      // 检查是否接收完成
      if (transmission.received === transmission.metadata.totalChunks) {
        assembleAndProcess(baseTopic, clientId, transmission);
        clientData.delete(baseTopic); // 清理已完成的数据
      }
    } catch (error) {
      console.error(`[Chunk] Error processing chunk from ${clientId}: ${error.message}`);
    }
    return;
  }

  // 结束标记处理
  if (topic.endsWith('/end')) {
    const baseTopic = topic.replace('/end', '');
    const clientData = chunkStorage.get(clientId);
    if (clientData && clientData.has(baseTopic)) {
      console.log(`[Chunk] Received end marker for ${baseTopic} from ${clientId}`);
      clientData.delete(baseTopic);
    }
    return;
  }
}

// 组装和处理完整数据
function assembleAndProcess(topic, clientId, transmission) {
  try {
    // 组装数据
    const assembledData = transmission.chunks.join('');
    
    // 验证数据大小
    if (assembledData.length !== transmission.metadata.totalSize) {
      throw new Error(`Size mismatch: expected ${transmission.metadata.totalSize}, got ${assembledData.length}`);
    }

    console.log(`[Chunk] Successfully assembled ${transmission.metadata.totalSize} bytes for ${topic}`);
    console.log(assembledData)
    // 根据主题类型处理数据
    switch(topic) {
      case 'device/xy-coords':
        handleXYMessage(assembledData, clientId);
        break;
      case 'device/frequency':
        handleFreqMessage(assembledData, clientId);
        break;
      case 'home/config':
        handleConfigMessage(assembledData, clientId);
        break;
      default:
        console.log(`[Chunk] Forwarding assembled data for ${topic}`);
        broadcast({
          type: 'message',
          topic,
          payload: assembledData,
          clientId,
          timestamp: new Date().toISOString(),
          assembled: true
        });
    }
  } catch (error) {
    console.error(`[Chunk] Assembly error for ${clientId} ${topic}: ${error.message}`);
  }
}

// MQTT message publish event
aedes.on('publish', (packet, client) => {
  if (!client) return;

  const topic = packet.topic;
  const payload = packet.payload.toString();
  
  // 优先处理分块传输
  if (topic.endsWith('/meta') || topic.endsWith('/chunk') || topic.endsWith('/end')) {
    handleChunkedData(client, packet);
    return;
  }

  // 原始处理逻辑保持不变
  console.log(`[MQTT] Received message on [${topic}]: ${payload.length > 100 ? payload.substring(0, 100) + '...' : payload}`);
  
  broadcast({ 
    type: 'message', 
    topic, 
    payload,
    clientId: client.id,
    timestamp: new Date().toISOString()
  });

  // 原始路由逻辑
  switch(topic) {
    case 'home/config':
      handleConfigMessage(payload, client.id);
      break;
    case 'device/xy-coords':
      handleXYMessage(payload, client.id);
      break;
    case 'device/frequency':
      handleFreqMessage(payload, client.id);
      break;
    default:
      console.log(`[MQTT] Unhandled topic message: ${topic}`);
  }
});

// Handle configuration messages
function handleConfigMessage(payload) {
  try {
    const config = JSON.parse(payload);
    console.log('[Config] Parsed configuration:');
    console.log(` - Device: ${config.device || 'Unknown'}`);
    console.log(` - Version: ${config.version || 'Unknown'}`);
    console.log(` - Interval: ${config.interval || 'Not set'}`);
    console.log(` - Status: ${config.status || 'Unknown'}`);
    
    broadcast({
      type: 'config',
      data: config,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('[Config] JSON parse error:', error.message);
    console.log('[Config] Raw data:', payload);
  }
}

// Handle coordinate messages
function handleXYMessage(payload, clientId) {
  try {
    const data = JSON.parse(payload);
    const coords = data.coordinates || data;
    
    console.log(`[Coordinates] From ${clientId}: Received ${coords.length} points`);
    
    broadcast({
      type: 'coordinates',
      data: coords,
      clientId,
      timestamp: new Date().toISOString()
    });
    
    // 新增：坐标数据处理逻辑
    if (Array.isArray(coords)) {
      const stats = {
        count: coords.length,
        firstPoint: coords[0],
        lastPoint: coords[coords.length - 1]
      };
      console.log(`[Coordinates] Processed ${stats.count} points`);
    }
    
  } catch (error) {
    console.error(`[Coordinates] Parse error from ${clientId}:`, error.message);
  }
}

// Handle frequency messages
function handleFreqMessage(payload) {
  try {
    const data = JSON.parse(payload);
    const freq = data.frequency;
    
    console.log(`[Frequency] Received: ${freq}Hz`);
    
    broadcast({
      type: 'frequency',
      data: freq,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('[Frequency] Parse error:', error.message);
    console.log('[Frequency] Raw data:', payload);
  }
}

// Get local IP address
function getLocalIP() {
  const interfaces = os.networkInterfaces();
  for (const name of Object.keys(interfaces)) {
    for (const iface of interfaces[name]) {
      if (iface.family === 'IPv4' && !iface.internal) {
        return iface.address;
      }
    }
  }
  return HOST;
}