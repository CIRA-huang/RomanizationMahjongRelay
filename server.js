'use strict';

// v1.1.0：Unity Host authoritative Join Flow；Relay 不再搶先判定遊戲房滿。

const http = require('http');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || '0.0.0.0';
const WS_PATH = process.env.WS_PATH || '/ws';

//
// v1.1.0:
// maxPlayers 是「Unity 遊戲房間容量提示」，不再由 Relay 直接判定房滿。
// 真正 2～4 人座位、保留 Seat、重新連線都交給 Unity Host。
// Relay 只保留獨立的 Socket 安全上限，避免單房無限制連線。
//
const RELAY_CLIENT_LIMIT_PER_ROOM =
  Math.max(
    4,
    Number(
      process.env.RELAY_CLIENT_LIMIT_PER_ROOM ||
      16
    )
  );

// roomCode -> { host: Peer|null, clients: Map<connectionId, Peer>, maxPlayers }
const rooms = new Map();

function normalizeRoomCode(value) {
  const room = String(value || 'ROOM').trim().toUpperCase();
  return room || 'ROOM';
}

function makeConnectionId(role) {
  return `${role === 'host' ? 'HOST' : 'PEER'}-${crypto.randomUUID().replace(/-/g, '')}`;
}

function sendJson(ws, value) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return false;
  try {
    ws.send(JSON.stringify(value));
    return true;
  } catch (error) {
    return false;
  }
}

function sendError(ws, reason) {
  sendJson(ws, {
    op: 'error',
    reason: String(reason || 'Relay error')
  });
}

function closeWithReason(ws, reason) {
  if (!ws) return;
  sendJson(ws, {
    op: 'kicked',
    reason: String(reason || 'Disconnected by relay')
  });
  try {
    ws.close(4000, String(reason || 'Disconnected').slice(0, 120));
  } catch (_) {
    try { ws.terminate(); } catch (_) {}
  }
}

function getPeer(ws) {
  return ws && ws._gamePeer ? ws._gamePeer : null;
}

function registerPeer(ws, message) {
  if (getPeer(ws)) {
    sendError(ws, 'This socket is already registered.');
    return;
  }

  const roomCode = normalizeRoomCode(message.roomCode);
  const role = String(message.role || '').toLowerCase();
  const requestedMax = Math.min(4, Math.max(2, Number(message.maxPlayers || 4)));

  if (role !== 'host' && role !== 'client') {
    sendError(ws, 'Invalid role.');
    return;
  }

  let room = rooms.get(roomCode);

  if (role === 'host') {
    if (room && room.host && room.host.ws.readyState === WebSocket.OPEN) {
      sendError(ws, 'Room already has an active host.');
      closeWithReason(ws, 'Room already exists');
      return;
    }

    if (!room) {
      room = {
        host: null,
        clients: new Map(),
        maxPlayers: requestedMax
      };
      rooms.set(roomCode, room);
    }

    room.maxPlayers = requestedMax;
  } else {
    if (!room || !room.host || room.host.ws.readyState !== WebSocket.OPEN) {
      sendJson(ws, {
        op: 'waiting_for_host',
        roomCode,
        reason: '房主尚未建立房間，Client 可保持等待。'
      });
      console.log(`[relay] client waiting for host room=${roomCode}`);
      return;
    }

    // v1.1.0：
    // 不在 Relay 層使用 room.maxPlayers 判斷「遊戲房滿」。
    //
    // 原因：
    // Unity Host 的 NetworkSessionManager 才知道：
    // - 真正可用 Seat
    // - PlayerId 保留座位
    // - 重新連線接管舊 Seat
    // - Client 離線轉 AI
    //
    // 若 Relay 先擋掉，Host 根本收不到 JoinRequest，
    // Client 也收不到正式 JoinRejected。
    if (room.clients.size >= RELAY_CLIENT_LIMIT_PER_ROOM) {
      sendError(ws, 'Relay room connection limit reached.');
      closeWithReason(ws, 'Relay room connection limit reached');
      return;
    }
  }

  const connectionId = makeConnectionId(role);
  const peer = {
    ws,
    role,
    roomCode,
    connectionId,
    suppressPeerLeft: false
  };

  ws._gamePeer = peer;

  if (role === 'host') {
    room.host = peer;
  } else {
    room.clients.set(connectionId, peer);
  }

  sendJson(ws, {
    op: 'registered',
    roomCode,
    role,
    connectionId,
    maxPlayers: room.maxPlayers
  });

  console.log(`[relay] registered ${role} ${connectionId} room=${roomCode}`);
}

function handleToHost(peer, message) {
  if (peer.role !== 'client') return;
  const room = rooms.get(peer.roomCode);
  if (!room || !room.host) return;

  sendJson(room.host.ws, {
    op: 'message',
    roomCode: peer.roomCode,
    sourceConnectionId: peer.connectionId,
    payloadJson: String(message.payloadJson || '')
  });
}

function handleBroadcast(peer, message) {
  if (peer.role !== 'host') return;
  const room = rooms.get(peer.roomCode);
  if (!room) return;

  for (const client of room.clients.values()) {
    sendJson(client.ws, {
      op: 'message',
      roomCode: peer.roomCode,
      sourceConnectionId: peer.connectionId,
      payloadJson: String(message.payloadJson || '')
    });
  }
}

function handleToClient(peer, message) {
  if (peer.role !== 'host') return;
  const room = rooms.get(peer.roomCode);
  if (!room) return;

  const targetId = String(message.targetConnectionId || '');
  const target = room.clients.get(targetId);
  if (!target) return;

  sendJson(target.ws, {
    op: 'message',
    roomCode: peer.roomCode,
    sourceConnectionId: peer.connectionId,
    payloadJson: String(message.payloadJson || '')
  });
}

function handleDisconnectClient(peer, message) {
  if (peer.role !== 'host') return;
  const room = rooms.get(peer.roomCode);
  if (!room) return;

  const targetId = String(message.targetConnectionId || '');
  const target = room.clients.get(targetId);
  if (!target) return;

  // v1.1.0：
  // Host 已經明確要求踢除此 Client（常見於 JoinRejected / 房滿）。
  // 先從 Relay Room 移除並標記 suppressPeerLeft，
  // 避免 Socket close 後 unregisterPeer() 又補送 peer_left，
  // 造成 Unity Host 收到「未註冊連線的離房訊息」。
  target.suppressPeerLeft = true;
  room.clients.delete(targetId);

  closeWithReason(
    target.ws,
    message.reason || 'Host disconnected this client'
  );
}

function unregisterPeer(ws) {
  const peer = getPeer(ws);
  if (!peer) return;

  ws._gamePeer = null;
  const room = rooms.get(peer.roomCode);
  if (!room) return;

  if (peer.role === 'host') {
    // v1.0.1：舊 Host Socket 的 close 事件可能比新 Host 重新註冊更晚到。
    // 若目前房間已經由另一個 Host connectionId 接管，這個舊 close
    // 絕對不能再把新 Host 與所有 Client 一起踢掉。
    if (!room.host || room.host.connectionId !== peer.connectionId) {
      console.log(`[relay] ignored stale host close ${peer.connectionId} room=${peer.roomCode}`);
      return;
    }

    room.host = null;

    for (const client of room.clients.values()) {
      sendJson(client.ws, {
        op: 'host_left',
        reason: '房主已離開中央 Relay。'
      });
      try { client.ws.close(4001, 'Host left'); } catch (_) {}
    }

    room.clients.clear();
    rooms.delete(peer.roomCode);
    console.log(`[relay] host left room=${peer.roomCode}`);
    return;
  }

  const actuallyRemoved =
    room.clients.delete(peer.connectionId);

  if (
    actuallyRemoved &&
    !peer.suppressPeerLeft &&
    room.host &&
    room.host.ws.readyState === WebSocket.OPEN
  ) {
    sendJson(room.host.ws, {
      op: 'peer_left',
      roomCode: peer.roomCode,
      sourceConnectionId: peer.connectionId,
      reason: 'Client socket closed'
    });
  }

  console.log(
    `[relay] client left ${peer.connectionId} room=${peer.roomCode}` +
    ` removed=${actuallyRemoved}` +
    ` suppressPeerLeft=${!!peer.suppressPeerLeft}`
  );
}

const httpServer = http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json; charset=utf-8' });
    const roomDetails = [];

    for (const [roomCode, room] of rooms.entries()) {
      roomDetails.push({
        roomCode,
        hasHost:
          !!(
            room.host &&
            room.host.ws.readyState === WebSocket.OPEN
          ),
        maxPlayers: room.maxPlayers,
        relayClients: room.clients.size
      });
    }

    res.end(JSON.stringify({
      ok: true,
      rooms: rooms.size,
      roomDetails
    }));
    return;
  }

  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Romanization Mahjong WebSocket Relay\n');
});

const wss = new WebSocketServer({
  server: httpServer,
  path: WS_PATH,
  maxPayload: 4 * 1024 * 1024
});

wss.on('connection', (ws) => {
  ws.on('message', (raw) => {
    let message;
    try {
      message = JSON.parse(raw.toString('utf8'));
    } catch (error) {
      sendError(ws, 'Invalid JSON.');
      return;
    }

    const op = String(message.op || '').toLowerCase();

    if (op === 'register') {
      registerPeer(ws, message);
      return;
    }

    const peer = getPeer(ws);
    if (!peer) {
      sendError(ws, 'Socket is not registered.');
      return;
    }

    switch (op) {
      case 'to_host':
        handleToHost(peer, message);
        break;
      case 'broadcast':
        handleBroadcast(peer, message);
        break;
      case 'to_client':
        handleToClient(peer, message);
        break;
      case 'disconnect_client':
        handleDisconnectClient(peer, message);
        break;
      case 'ping':
        sendJson(ws, { op: 'pong' });
        break;
      default:
        sendError(ws, `Unknown op: ${op}`);
        break;
    }
  });

  ws.on('close', () => unregisterPeer(ws));
  ws.on('error', () => unregisterPeer(ws));
});

httpServer.listen(PORT, HOST, () => {
  console.log(`[relay] HTTP/WebSocket listening on ${HOST}:${PORT}${WS_PATH}`);
  console.log(`[relay] health: http://127.0.0.1:${PORT}/health`);
});
