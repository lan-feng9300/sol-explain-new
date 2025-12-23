import WebSocket from 'ws';
import { Connection, PublicKey } from '@solana/web3.js';
import { config } from '../config/index.js';
import { parseMultipleTradeInfo } from './transactionParseService.js';

/**
 * dRPC WebSocket 地址监听服务
 * 使用 accountSubscribe 监听地址变化，并解析新交易
 */
class AddressMonitorService {
  constructor() {
    this.connections = new Map(); // 存储多个连接，key 为 address
    this.requestIdCounter = 0; // JSON-RPC 2.0 请求 ID 计数器
  }

  /**
   * 生成 JSON-RPC 2.0 请求 ID
   */
  getNextRequestId() {
    return ++this.requestIdCounter;
  }

  /**
   * 获取 dRPC WebSocket URL
   */
  getWebSocketUrl() {
    if (!config.drpc.apiKey) {
      throw new Error('dRPC API Key 未配置，请在 .env 文件中设置 DRPC_API_KEY');
    }
    // dRPC WebSocket URL 格式: wss://lb.drpc.live/solana/{apiKey}
    return `wss://lb.drpc.live/solana/${config.drpc.apiKey}`;
  }

  /**
   * 获取 Connection 实例（用于获取交易签名）
   */
  getConnection() {
    if (!config.drpc.apiKey) {
      throw new Error('dRPC API Key 未配置');
    }
    const rpcEndpoint = `https://lb.drpc.live/solana/${config.drpc.apiKey}`;
    return new Connection(rpcEndpoint, 'confirmed');
  }

  /**
   * 创建并设置 WebSocket 客户端
   */
  createClient(address, connectionInfo) {
    const wsUrl = this.getWebSocketUrl();
    const ws = new WebSocket(wsUrl);

    ws.on('open', () => {
      console.log(`dRPC WebSocket 连接时间: ${new Date().toISOString()}`);
      
      connectionInfo.ws = ws;
      connectionInfo.connected = true;
      connectionInfo.connectedAt = new Date();
      connectionInfo.lastMessageTime = new Date(); // 记录最后收到消息的时间

      // 发送订阅请求（accountSubscribe）
      const requestId = this.getNextRequestId();
      const subscribeMsg = {
        jsonrpc: "2.0",
        id: requestId,
        method: "accountSubscribe",
        params: [
          address,
          {
            encoding: connectionInfo.encoding || "jsonParsed",
            commitment: connectionInfo.commitment || "confirmed"
          }
        ]
      };

      connectionInfo.subscribeRequestId = requestId;
      ws.send(JSON.stringify(subscribeMsg));
      console.log(`✅ 已发送账户订阅请求 (${address}):`, JSON.stringify(subscribeMsg));

      // 启动主动心跳（每20秒发送一次 ping 保持连接）
      connectionInfo.pingInterval = setInterval(() => {
        if (connectionInfo.ws && connectionInfo.ws.readyState === 1) { // WebSocket.OPEN = 1
          try {
            connectionInfo.ws.ping();
          } catch (error) {
            console.error(`❌ 发送心跳失败 (${address}):`, error);
          }
        }
      }, 20000); // 每20秒发送一次 ping

      // 启动心跳检测（每30秒检查一次连接状态）
      connectionInfo.heartbeatInterval = setInterval(() => {
        this.checkConnectionHealth(address, connectionInfo);
      }, 30000); // 30秒检查一次

      // 定期刷新订阅（每1.6分钟重新订阅一次，防止订阅失效）
      // 注意：dRPC 的订阅可能有时效性，需要定期刷新
      connectionInfo.refreshSubscriptionInterval = setInterval(() => {
        this.refreshSubscription(address, connectionInfo);
      }, 96000); // 1.6分钟（96秒）刷新一次订阅，确保在2分钟失效前刷新
    });

    ws.on('message', (data) => {
      try {
        // 更新最后收到消息的时间
        connectionInfo.lastMessageTime = new Date();
        
        const message = JSON.parse(data.toString());
        console.log(`\n📥 WebSocket 收到原始消息 (${address}):`, data.toString().substring(0, 200));
        this.handleMessage(address, message, connectionInfo);
      } catch (err) {
        console.error(`❌ 解析 WebSocket 消息失败 (${address}):`, err);
        console.error(`原始消息内容:`, data.toString());
      }
    });

    ws.on('error', (error) => {
      console.error(`❌ dRPC WebSocket 错误 (${address}):`, error.toString());
      console.error(`   错误详情:`, error);
      connectionInfo.connected = false;
    });

    ws.on('close', (code, reason) => {
      console.log(`❌ dRPC WebSocket 连接已关闭 (${address}). Code: ${code}, Reason: ${reason}`);
      console.log(`   关闭时间: ${new Date().toISOString()}`);
      if (connectionInfo.connectedAt) {
        const duration = (new Date() - connectionInfo.connectedAt) / 1000;
        console.log(`   连接持续时间: ${duration.toFixed(2)} 秒`);
      }
      connectionInfo.connected = false;
      
      // 清除所有定时器
      if (connectionInfo.pingInterval) {
        clearInterval(connectionInfo.pingInterval);
        connectionInfo.pingInterval = null;
      }
      if (connectionInfo.heartbeatInterval) {
        clearInterval(connectionInfo.heartbeatInterval);
        connectionInfo.heartbeatInterval = null;
      }
      if (connectionInfo.refreshSubscriptionInterval) {
        clearInterval(connectionInfo.refreshSubscriptionInterval);
        connectionInfo.refreshSubscriptionInterval = null;
      }
      
      this.cleanupConnection(address);
      // 注意：连接关闭后不会自动重连，需要手动重新订阅
      console.log(`⚠️ 提示: 连接已关闭，如需继续监控，请重新订阅地址 ${address}`);
    });

    ws.on('ping', () => {
      ws.pong();
      // 更新最后收到消息的时间（ping 也算消息）
      connectionInfo.lastMessageTime = new Date();
    });

    ws.on('pong', () => {
      // 更新最后收到消息的时间（pong 也算消息）
      connectionInfo.lastMessageTime = new Date();
    });

    return ws;
  }

  /**
   * 处理收到的消息（JSON-RPC 2.0 格式）
   */
  handleMessage(address, data, connectionInfo) {
    // JSON-RPC 2.0 响应（订阅成功）
    if (data.id && data.id === connectionInfo.subscribeRequestId) {
      if (data.result) {
        // 订阅成功，result 是订阅 ID
        const oldSubscriptionId = connectionInfo.subscriptionId;
        connectionInfo.subscriptionId = data.result;
        this.connections.set(address, connectionInfo);

        // 更新最后收到消息的时间
        connectionInfo.lastMessageTime = new Date();
        
        // 解析 Promise（只在首次订阅时）
        if (connectionInfo.resolve && !oldSubscriptionId) {
          connectionInfo.resolve(connectionInfo);
        }
      } else if (data.error) {
        console.error(`❌ 订阅失败 (${address}):`, data.error);
        if (connectionInfo.reject) {
          connectionInfo.reject(new Error(data.error.message || '订阅失败'));
        }
      }
      return;
    }

    // JSON-RPC 2.0 通知（账户变化）
    if (data.method === 'accountNotification') {
      console.log(`🔔 检测到账户变化通知 (${address})，准备处理...`);
      this.handleAccountNotification(address, data, connectionInfo);
      return;
    }

    // 其他消息类型
    console.log(`ℹ️ 收到其他类型消息 (${address}):`, data.method || 'unknown', data);
  }

  /**
   * 处理账户变化通知
   */
  async handleAccountNotification(address, data, connectionInfo) {
    const accountInfo = data.params?.result?.value;
    const context = data.params?.result?.context;
    const slot = context?.slot;

    console.log(`Owner: ${accountInfo?.owner}`);
    console.log(`已处理签名数量: ${connectionInfo.lastProcessedSignatures?.size || 0}`);

    // 检查连接状态
    if (!connectionInfo.connected) {
      console.error(`⚠️ 警告: 收到账户变化通知，但连接状态为未连接 (${address})`);
      return;
    }

    // 获取最新的交易签名
    try {
      await this.fetchAndParseNewTransactions(address, connectionInfo, slot);
    } catch (error) {
      console.error(`处理账户变化失败 (${address}):`, error);
    }
  }

  /**
   * 获取并解析新交易
   */
  async fetchAndParseNewTransactions(address, connectionInfo, currentSlot) {
    try {
      const connection = this.getConnection();
      const publicKey = new PublicKey(address);

      // 获取最新的交易签名（限制 10 个，避免过多）
      const signatures = await connection.getSignaturesForAddress(publicKey, {
        limit: 10
      });

      if (!signatures || signatures.length === 0) {
        console.log(`未找到新交易 (${address})`);
        return;
      }

      // 获取上次处理的签名集合
      const lastProcessedSignatures = connectionInfo.lastProcessedSignatures || new Set();
      
      // 打印调试信息
      console.log(`  已处理签名数量: ${lastProcessedSignatures.size}`);
      if (signatures.length > 0) {
        const latestSig = signatures[0];
        console.log(`  最新交易签名: ${latestSig.signature.substring(0, 16)}...`);
        console.log(`  最新交易时间: ${latestSig.blockTime ? new Date(latestSig.blockTime * 1000).toISOString() : 'N/A'}`);
      }
      
      // 找出新的交易签名
      const newSignatures = signatures
        .map(sig => sig.signature)
        .filter(sig => !lastProcessedSignatures.has(sig));

      if (newSignatures.length === 0) {
        console.log(`⚠️ 没有新交易需要处理 (${address})`);
        console.log(`  原因: 所有 ${signatures.length} 个交易都已在已处理列表中`);
        // 打印前几个已处理的签名，帮助调试
        if (signatures.length > 0) {
          const firstFew = signatures.slice(0, 3).map(s => s.signature);
          console.log(`  这些签名是否在已处理列表中: ${firstFew.map(s => lastProcessedSignatures.has(s)).join(', ')}`);
        }
        return;
      }

      console.log(`✅ 发现 ${newSignatures.length} 个新交易 (${address})`);

      // 更新已处理的签名集合
      newSignatures.forEach(sig => lastProcessedSignatures.add(sig));
      // 只保留最近的 100 个签名，避免内存泄漏
      if (lastProcessedSignatures.size > 100) {
        const signaturesArray = Array.from(lastProcessedSignatures);
        lastProcessedSignatures.clear();
        signaturesArray.slice(-50).forEach(sig => lastProcessedSignatures.add(sig));
      }
      connectionInfo.lastProcessedSignatures = lastProcessedSignatures;

      // 批量解析交易（复用现有逻辑）
      const tradeInfoMap = await parseMultipleTradeInfo(newSignatures, 5);

      // 处理解析结果
      const results = [];
      for (const signature of newSignatures) {
        const tradeInfo = tradeInfoMap.get(signature);
        const signatureInfo = signatures.find(s => s.signature === signature);

        const result = {
          address,
          signature,
          slot: signatureInfo?.slot || currentSlot,
          blockTime: signatureInfo?.blockTime ? new Date(signatureInfo.blockTime * 1000).toISOString() : null,
          error: signatureInfo?.err || null,
          tradeInfo: tradeInfo || null,
          // 如果没有解析出交易信息，可能是普通转账或其他类型
          hasTradeInfo: !!tradeInfo
        };

        // 如果是 swap 交易，添加代币合约地址信息
        if (tradeInfo) {
          // 根据交易类型确定 tokenA 和 tokenB
          // tokenA: 输入代币（从哪个代币 swap）
          // tokenB: 输出代币（swap 到哪个代币）
          // 对于 'buy': tokenA = SOL (soldToken), tokenB = Token (boughtToken)
          // 对于 'sell': tokenA = Token (soldToken), tokenB = SOL (boughtToken)
          // 对于 'swap': tokenA = 输入代币 (soldToken), tokenB = 输出代币 (boughtToken)
          result.swap = {
            type: tradeInfo.type, // 'buy', 'sell', 'swap'
            dex: tradeInfo.dex,
            // Token A (输入代币/卖出的代币)
            tokenA: {
              mint: tradeInfo.soldToken?.mint || null, // 代币合约地址 (CA)
              symbol: tradeInfo.soldToken?.symbol || 'Unknown',
              amount: tradeInfo.soldToken?.amount || 0,
              decimals: tradeInfo.soldToken?.decimals || 0,
              ca: tradeInfo.soldToken?.mint || null // 合约地址别名
            },
            // Token B (输出代币/买入的代币)
            tokenB: {
              mint: tradeInfo.boughtToken?.mint || null, // 代币合约地址 (CA)
              symbol: tradeInfo.boughtToken?.symbol || 'Unknown',
              amount: tradeInfo.boughtToken?.amount || 0,
              decimals: tradeInfo.boughtToken?.decimals || 0,
              ca: tradeInfo.boughtToken?.mint || null // 合约地址别名
            },
            price: tradeInfo.price || null,
            fee: tradeInfo.fee || null
          };
        }

        results.push(result);

        // 打印交易信息
        if (tradeInfo) {
          console.log(`\n✅ 解析到交易 (${address}):`);
          console.log(`  签名: ${signature.substring(0, 16)}...`);
          console.log(`  类型: ${tradeInfo.type}`);
          console.log(`  DEX: ${tradeInfo.dex}`);
          console.log(`  卖出: ${tradeInfo.soldToken?.symbol || 'Unknown'} ${tradeInfo.soldToken?.amount || 0}`);
          console.log(`  卖出代币CA: ${tradeInfo.soldToken?.mint || 'N/A'}`);
          console.log(`  买入: ${tradeInfo.boughtToken?.symbol || 'Unknown'} ${tradeInfo.boughtToken?.amount || 0}`);
          console.log(`  买入代币CA: ${tradeInfo.boughtToken?.mint || 'N/A'}`);
        } else {
          console.log(`\nℹ️  普通交易 (${address}): ${signature.substring(0, 16)}...`);
        }
      }

      // 触发回调函数（如果已注册）
      if (connectionInfo.onTransaction) {
        for (const result of results) {
          connectionInfo.onTransaction(result);
        }
      }

      return results;
    } catch (error) {
      console.error(`获取新交易失败 (${address}):`, error);
      throw error;
    }
  }

  /**
   * 刷新订阅（重新发送订阅请求，防止订阅失效）
   */
  async refreshSubscription(address, connectionInfo) {
    if (!connectionInfo.connected || !connectionInfo.ws || connectionInfo.ws.readyState !== 1) {
      console.log(`⚠️ 无法刷新订阅 (${address}): 连接未建立`);
      return;
    }

    try {
      console.log(`开始刷新订阅(${address}), 当前时间: ${new Date().toISOString()}`);
      
      // 先取消旧订阅（如果有）
      if (connectionInfo.subscriptionId) {
        const unsubscribeRequestId = this.getNextRequestId();
        const unsubscribeMsg = {
          jsonrpc: "2.0",
          id: unsubscribeRequestId,
          method: "accountUnsubscribe",
          params: [connectionInfo.subscriptionId]
        };
        connectionInfo.ws.send(JSON.stringify(unsubscribeMsg));
        
        // 等待一小段时间再重新订阅
        await new Promise(resolve => setTimeout(resolve, 500));
      }

      // 重新发送订阅请求
      const requestId = this.getNextRequestId();
      const subscribeMsg = {
        jsonrpc: "2.0",
        id: requestId,
        method: "accountSubscribe",
        params: [
          address,
          {
            encoding: connectionInfo.encoding || "jsonParsed",
            commitment: connectionInfo.commitment || "confirmed"
          }
        ]
      };

      connectionInfo.subscribeRequestId = requestId;
      connectionInfo.ws.send(JSON.stringify(subscribeMsg));
      console.log(`✅ 已重新发送账户订阅请求 (${address}), 请求ID: ${requestId}`);
      
      // 重置订阅ID，等待新的订阅响应
      connectionInfo.subscriptionId = null;
      connectionInfo.lastSubscriptionRefresh = new Date();
    } catch (error) {
      console.error(`❌ 刷新订阅失败 (${address}):`, error);
    }
  }

  /**
   * 检查连接健康状态
   */
  checkConnectionHealth(address, connectionInfo) {
    if (!connectionInfo.connected) {
      console.log(`⚠️ 连接健康检查: 连接状态为未连接 (${address})`);
      return;
    }

    const now = new Date();
    const lastMessageTime = connectionInfo.lastMessageTime || connectionInfo.connectedAt;
    const timeSinceLastMessage = (now - lastMessageTime) / 1000; // 秒

    // 检查 WebSocket 实际状态
    if (connectionInfo.ws) {
      const readyState = connectionInfo.ws.readyState;
      if (readyState === 2 || readyState === 3) {
        console.error(`❌ WebSocket 实际状态为关闭 (${address}), readyState: ${readyState}`);
        connectionInfo.connected = false;
        this.cleanupConnection(address);
        return;
      }
    }

    // 如果超过2分钟没有收到任何消息，可能是连接有问题
    if (timeSinceLastMessage > 120) {
      console.warn(`⚠️ 警告: 超过2分钟没有收到消息 (${address})`);
      console.warn(`   可能原因: 连接已断开但未触发 close 事件，或订阅已失效`);
      console.warn(`   建议: 检查网络连接或重新订阅`);
      
      // 如果超过5分钟没有收到账户变化通知，主动刷新订阅
      if (timeSinceLastMessage > 300 && connectionInfo.subscriptionId) {
        console.warn(`⚠️ 超过5分钟没有收到账户变化通知，尝试刷新订阅 (${address})`);
        this.refreshSubscription(address, connectionInfo);
      }
    }
  }

  /**
   * 清理连接
   */
  cleanupConnection(address) {
    const connectionInfo = this.connections.get(address);
    if (connectionInfo) {
      if (connectionInfo.autoCloseTimer) {
        clearTimeout(connectionInfo.autoCloseTimer);
      }
      if (connectionInfo.pingInterval) {
        clearInterval(connectionInfo.pingInterval);
        connectionInfo.pingInterval = null;
      }
      if (connectionInfo.heartbeatInterval) {
        clearInterval(connectionInfo.heartbeatInterval);
        connectionInfo.heartbeatInterval = null;
      }
      if (connectionInfo.refreshSubscriptionInterval) {
        clearInterval(connectionInfo.refreshSubscriptionInterval);
        connectionInfo.refreshSubscriptionInterval = null;
      }
      if (connectionInfo.ws && connectionInfo.ws.readyState === 1) { // WebSocket.OPEN = 1
        // 如果有订阅 ID，发送取消订阅请求
        if (connectionInfo.subscriptionId) {
          const requestId = this.getNextRequestId();
          const unsubscribeMsg = {
            jsonrpc: "2.0",
            id: requestId,
            method: "accountUnsubscribe",
            params: [connectionInfo.subscriptionId]
          };
          connectionInfo.ws.send(JSON.stringify(unsubscribeMsg));
        }
        connectionInfo.ws.close();
      }
      this.connections.delete(address);
    }
  }

  /**
   * 订阅地址变化监听
   * @param {string} address - 账户地址
   * @param {string} encoding - 编码格式（jsonParsed, base58, base64）
   * @param {string} commitment - 确认级别（finalized, confirmed, processed）
   * @param {Function} onTransaction - 新交易回调函数
   * @param {number} autoCloseAfter - 自动关闭时间（毫秒），0 表示不自动关闭
   * @returns {Promise<Object>} 连接信息
   */
  async subscribeAddress(
    address,
    encoding = 'jsonParsed',
    commitment = 'confirmed',
    onTransaction = null,
    autoCloseAfter = 0
  ) {
    // 检查是否已有连接
    if (this.connections.has(address)) {
      const existing = this.connections.get(address);
      if (existing.connected) {
        console.log(`地址 ${address} 已有活跃连接`);
        return existing;
      } else {
        // 清理无效连接
        this.cleanupConnection(address);
      }
    }

    // 检查 API Key
    if (!config.drpc.apiKey) {
      throw new Error('dRPC API Key 未配置，请在 .env 文件中设置 DRPC_API_KEY');
    }

    return new Promise((resolve, reject) => {
      const connectionInfo = {
        address,
        encoding,
        commitment,
        ws: null,
        connected: false,
        subscriptionId: null,
        subscribeRequestId: null,
        onTransaction,
        lastProcessedSignatures: new Set(),
        autoCloseTimer: null,
        autoCloseAfter,
        createdAt: new Date(),
        resolve,
        reject
      };

      // 创建 WebSocket 客户端， 这个地方做了消息处理逻辑
      const ws = this.createClient(address, connectionInfo);
      this.connections.set(address, connectionInfo);

      // 设置连接超时
      setTimeout(() => {
        if (!connectionInfo.connected) {
          this.cleanupConnection(address);
          reject(new Error('WebSocket 连接超时'));
        }
      }, 10000); // 10 秒超时

      // 设置自动关闭定时器
      if (autoCloseAfter > 0) {
        connectionInfo.autoCloseTimer = setTimeout(() => {
          this.unsubscribeAddress(address);
          console.log(`连接已自动关闭（${autoCloseAfter / 1000} 秒后）`);
        }, autoCloseAfter);
      }
    });
  }

  /**
   * 取消订阅地址监听
   * @param {string} address - 账户地址
   */
  unsubscribeAddress(address) {
    this.cleanupConnection(address);
    console.log(`已取消订阅地址 ${address}`);
  }

  /**
   * 获取所有活跃连接
   */
  getActiveConnections() {
    const active = [];
    const now = new Date();
    
    for (const [address, info] of this.connections.entries()) {
      // 检查连接状态
      let isActuallyConnected = false;
      
      if (info.connected && info.ws) {
        const readyState = info.ws.readyState;
        // readyState: 0=CONNECTING, 1=OPEN, 2=CLOSING, 3=CLOSED
        isActuallyConnected = (readyState === 1); // 只有 OPEN 状态才算真正连接
        
        // 如果标志为已连接但实际状态不是 OPEN，更新状态
        if (!isActuallyConnected) {
          console.log(`⚠️ 检测到连接状态不一致 (${address}): connected=${info.connected}, readyState=${readyState}`);
          info.connected = false;
          // 清理无效连接
          this.cleanupConnection(address);
        }
      }
      
      if (isActuallyConnected) {
        const lastMessageTime = info.lastMessageTime || info.connectedAt;
        const timeSinceLastMessage = lastMessageTime ? (now - lastMessageTime) / 1000 : 0;
        
        active.push({
          address,
          subscriptionId: info.subscriptionId,
          encoding: info.encoding,
          commitment: info.commitment,
          createdAt: info.createdAt,
          connectedAt: info.connectedAt,
          lastMessageTime: info.lastMessageTime,
          timeSinceLastMessage: Math.round(timeSinceLastMessage), // 秒
          readyState: info.ws?.readyState || 'N/A'
        });
      } else {
        // 清理无效连接
        if (this.connections.has(address)) {
          console.log(`清理无效连接 (${address})`);
          this.cleanupConnection(address);
        }
      }
    }
    return active;
  }

  /**
   * 关闭所有连接
   */
  closeAllConnections() {
    for (const [address] of this.connections.entries()) {
      this.unsubscribeAddress(address);
    }
  }
}

// 导出单例实例
export const addressMonitorService = new AddressMonitorService();

