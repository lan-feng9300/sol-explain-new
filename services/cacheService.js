import Redis from 'ioredis';
import {config} from '../config/index.js';

// ============================
// 模块级变量（仅在此处声明）
// ============================
let solPriceFetchPromise = null;
let lastFetchTime = 0;
let lastErrorLogTime = 0;
const FETCH_COOLDOWN = 60000; // 1分钟内不重复获取
const ERROR_LOG_COOLDOWN = 300000; // 5分钟内不重复记录错误

// Redis客户端单例
let redisClient = null;

/**
 * 获取 RPC URL（优先使用 dRPC，备选 Helius 或公共 RPC）
 * @param {string} apiKey - Helius API Key（可选，如果 dRPC 不可用时使用）
 * @returns {string} RPC URL
 */
function getRpcUrl(apiKey = null) {
  // 优先使用 dRPC
  if (config.drpc.apiKey) {
    return `https://lb.drpc.live/solana/${config.drpc.apiKey}`;
  }
  
  // 如果没有 dRPC，使用 Helius（如果提供了 apiKey）
  if (apiKey) {
    return `https://mainnet.helius-rpc.com/?api-key=${apiKey}`;
  }
  
  // 最后使用公共 RPC
  return 'https://api.mainnet-beta.solana.com';
}

/**
 * 初始化 Redis 客户端（单例模式）
 */
function initRedisClient() {
  if (redisClient) {
    return redisClient;
  }

  try {
    redisClient = new Redis({
      host: config.redis.HOST,
      port: config.redis.PORT,
      password: config.redis.PASSWORD || undefined,
      retryStrategy: (times) => {
        // 重试策略：最多重试3次
        if (times > 3) {
          return null; // 停止重试
        }
        return Math.min(times * 50, 2000);
      },
    });

    redisClient.on('error', (err) => {
      console.error('Redis 客户端错误:', err);
      redisClient = null;
    });

    redisClient.on('connect', () => {
      console.log('✅ Redis 缓存服务连接成功');
    });

    return redisClient;
  } catch (error) {
    console.warn('Redis 连接失败，缓存功能将不可用:', error.message);
    return null;
  }
}

/**
 * 通用缓存服务类
 */
class CacheService {
  /**
   * 获取缓存值
   * @param {string} key - 缓存键
   * @returns {Promise<any|null>} 缓存值，如果不存在则返回 null
   */
  static async get(key) {
    try {
      const client = initRedisClient();
      if (!client) {
        return null;
      }

      const data = await client.get(key);
      if (data) {
        return JSON.parse(data);
      }
      return null;
    } catch (error) {
      console.warn(`从 Redis 获取缓存失败 (key: ${key}):`, error.message);
      return null;
    }
  }

  /**
   * 设置缓存值
   * @param {string} key - 缓存键
   * @param {any} value - 缓存值
   * @param {number} ttl - 过期时间（秒），默认 3600（1小时）
   * @returns {Promise<boolean>} 是否设置成功
   */
  static async set(key, value, ttl = 3600) {
    try {
      const client = initRedisClient();
      if (!client) {
        return false;
      }

      await client.setex(key, ttl, JSON.stringify(value));
      return true;
    } catch (error) {
      console.warn(`设置 Redis 缓存失败 (key: ${key}):`, error.message);
      return false;
    }
  }

  /**
   * 删除缓存
   * @param {string} key - 缓存键
   * @returns {Promise<boolean>} 是否删除成功
   */
  static async delete(key) {
    try {
      const client = initRedisClient();
      if (!client) {
        return false;
      }

      await client.del(key);
      return true;
    } catch (error) {
      console.warn(`删除 Redis 缓存失败 (key: ${key}):`, error.message);
      return false;
    }
  }

  /**
   * 检查缓存是否存在
   * @param {string} key - 缓存键
   * @returns {Promise<boolean>} 是否存在
   */
  static async exists(key) {
    try {
      const client = initRedisClient();
      if (!client) {
        return false;
      }

      const result = await client.exists(key);
      return result === 1;
    } catch (error) {
      console.warn(`检查 Redis 缓存失败 (key: ${key}):`, error.message);
      return false;
    }
  }

  /**
   * 获取或设置缓存（如果不存在则调用函数获取并缓存）
   * @param {string} key - 缓存键
   * @param {Function} fetchFn - 获取数据的函数
   * @param {number} ttl - 过期时间（秒），默认 3600（1小时）
   * @returns {Promise<any>} 缓存值或新获取的值
   */
  static async getOrSet(key, fetchFn, ttl = 3600) {
    // 先尝试从缓存获取
    const cached = await this.get(key);
    if (cached !== null) {
      return cached;
    }

    // 缓存不存在，调用函数获取
    try {
      const value = await fetchFn();
      
      // 获取成功，保存到缓存
      await this.set(key, value, ttl);
      
      return value;
    } catch (error) {
      // 获取失败，抛出错误
      throw error;
    }
  }
}

/**
 * 生成代币总供应量的缓存键
 * @param {string} tokenAddress - 代币地址
 * @returns {string} 缓存键
 */
export function getTokenTotalSupplyCacheKey(tokenAddress) {
  return `ca:${tokenAddress}:tokenTotalSupply`;
}

/**
 * 从 Solscan API 获取代币元数据（包括符号和名称）
 * @param {string} tokenMintAddress - 代币的 mint 地址
 * @returns {Promise<{symbol: string, name: string}>} 代币元数据
 */
async function getTokenMetadataFromSolscan(tokenMintAddress) {
  try {
    const url = `https://public-api.solscan.io/token/meta?tokenAddress=${tokenMintAddress}`;
    
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 5000);
    
    const response = await fetch(url, {
      headers: {
        'Accept': 'application/json',
        'User-Agent': 'Mozilla/5.0'
      },
      signal: controller.signal
    });
    
    clearTimeout(timeoutId);
    
    if (!response.ok) {
      return { symbol: '', name: '' };
    }
    
    const data = await response.json();
    return {
      symbol: data.symbol || data.tokenSymbol || '',
      name: data.name || data.tokenName || ''
    };
  } catch (error) {
    console.log('从 Solscan 获取代币元数据失败:', error.message);
    return { symbol: '', name: '' };
  }
}

/**
 * 获取代币的总供应量（Total Supply）和元数据
 * @param {string} apiKey - Helius API Key（可选）
 * @param {string} tokenMintAddress - 代币的 mint 地址
 * @returns {Promise<{supply: string, uiSupply: number, decimals: number, symbol: string, name: string}>} 总供应量信息和元数据
 */
export async function getTokenTotalSupply(apiKey, tokenMintAddress) {
  try {
    // 优先使用 dRPC，备选 Helius 或公共 RPC
    const rpcUrl = getRpcUrl(apiKey);
    const rpcProvider = config.drpc.apiKey ? 'dRPC' : (apiKey ? 'Helius' : 'Public RPC');
    
    console.log(`使用 ${rpcProvider} 获取代币总供应量 (${tokenMintAddress})...`);
    
    // 获取 mint 账户信息（使用标准 Solana RPC 方法 getAccountInfo）
    const response = await fetch(rpcUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: '1',
        method: 'getAccountInfo',
        params: [tokenMintAddress, { encoding: 'jsonParsed' }]
      })
    });
    
    const data = await response.json();
    
    if (data.error) {
      throw new Error(`RPC 错误: ${data.error.message}`);
    }
    
    if (!data.result?.value?.data?.parsed?.info) {
      throw new Error('无法获取代币信息');
    }
    
    const mintInfo = data.result.value.data.parsed.info;
    const supply = mintInfo.supply || '0';
    const decimals = mintInfo.decimals || 9;
    const uiSupply = Number(supply) / Math.pow(10, decimals);
    
    // 尝试从 Solscan 获取代币符号和名称
    const metadata = await getTokenMetadataFromSolscan(tokenMintAddress);
    
    return {
      supply: supply.toString(),
      uiSupply: uiSupply,
      decimals: decimals,
      symbol: metadata.symbol,
      name: metadata.name
    };
  } catch (error) {
    console.error('获取代币总供应量失败:', error);
    throw error;
  }
}

/**
 * 获取代币总供应量（带缓存）
 * @param {string} apiKey - Helius API Key
 * @param {string} tokenMintAddress - 代币地址
 * @param {number} cacheTtl - 缓存过期时间（秒），默认 3600（1小时）
 * @returns {Promise<Object>} 代币总供应量信息
 */
export async function getTokenTotalSupplyWithCache(apiKey, tokenMintAddress, cacheTtl = 3600) {
  const cacheKey = getTokenTotalSupplyCacheKey(tokenMintAddress);
  
  return await CacheService.getOrSet(
    cacheKey,
    async () => {
      // 直接调用本地的 getTokenTotalSupply 函数
      return await getTokenTotalSupply(apiKey, tokenMintAddress);
    },
    cacheTtl
  );
}

/**
 * 生成代币元数据的缓存键
 * @param {string} tokenAddress - 代币地址
 * @returns {string} 缓存键
 */
function getTokenMetadataCacheKey(tokenAddress) {
  return `token:metadata:${tokenAddress}`;
}

/**
 * 使用 Helius DAS API getAsset 获取单个代币元数据（带缓存）
 * @param {string} apiKey - Helius API Key
 * @param {string} tokenAddress - 代币地址
 * @param {number} cacheTtl - 缓存过期时间（秒），默认 259200（3天）
 * @returns {Promise<{address: string, symbol: string, supply: string, decimals: number} | null>} 代币元数据
 */
export async function getTokenMetadataViaHelius(apiKey, tokenAddress, cacheTtl = 259200) {
  const cacheKey = getTokenMetadataCacheKey(tokenAddress);
  
  // 先尝试从缓存获取
  const cached = await CacheService.get(cacheKey);
  if (cached) {
    console.log(`从缓存获取代币 ${tokenAddress} 元数据`);
    return JSON.parse(cached);
  }

  try {
    const heliusRpcUrl = `https://mainnet.helius-rpc.com/?api-key=${apiKey}`;

    const requestBody = {
      jsonrpc: '2.0',
      id: '1',
      method: 'getAsset',
      params: {
        id: tokenAddress,
        displayOptions: {
          showFungible: true
        }
      }
    };

    const response = await fetch(heliusRpcUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
      throw new Error(`HTTP ${response.status}`);
    }

    const data = await response.json();

    if (data.error) {
      throw new Error(data.error.message || JSON.stringify(data.error));
    }

    const asset = data.result;
    if (!asset) {
      return null;
    }

    // 提取所需字段
    const tokenInfo = asset.token_info || {};
    const symbol = tokenInfo.symbol || '';
    const supply = tokenInfo.supply || '0';
    const decimals = tokenInfo.decimals || 9;

    const result = {
      address: tokenAddress,
      symbol: symbol,
      supply: supply.toString(),
      decimals: decimals
    };

    // 保存到缓存
    await CacheService.set(cacheKey, JSON.stringify(result), cacheTtl);

    return result;
  } catch (error) {
    console.error(`获取代币 ${tokenAddress} 元数据失败:`, error.message);
    return null;
  }
}

/**
 * 批量获取代币元数据（使用 Helius DAS API getAsset，带缓存）
 * @param {string} apiKey - Helius API Key
 * @param {string[]} tokenAddresses - 代币地址数组
 * @param {number} cacheTtl - 缓存过期时间（秒），默认 259200（3天）
 * @returns {Promise<Array<{address: string, symbol: string, supply: string, decimals: number}>>} 代币元数据数组
 */
export async function getTokenMetadataMultipleViaHelius(apiKey, tokenAddresses, cacheTtl = 259200) {
  try {
    // 先批量检查缓存
    const cacheKeys = tokenAddresses.map(addr => getTokenMetadataCacheKey(addr));
    const cachedResults = await Promise.all(
      cacheKeys.map(async (cacheKey, index) => {
        const cached = await CacheService.get(cacheKey);
        if (cached) {
          return {
            address: tokenAddresses[index],
            data: JSON.parse(cached),
            fromCache: true
          };
        }
        return {
          address: tokenAddresses[index],
          data: null,
          fromCache: false
        };
      })
    );

    // 分离缓存命中和未命中的地址
    const cachedItems = cachedResults.filter(item => item.fromCache).map(item => item.data);
    const uncachedAddresses = cachedResults
      .filter(item => !item.fromCache)
      .map(item => item.address);

    // 只对未缓存的地址调用 API（并发调用，但控制并发数避免过载）
    let apiResults = [];
    if (uncachedAddresses.length > 0) {
      // 对于少量地址，直接并发；对于大量地址，分批处理
      const concurrency = Math.min(uncachedAddresses.length, 10); // 最多10个并发
      
      if (uncachedAddresses.length <= concurrency) {
        // 少量地址，直接并发
        apiResults = await Promise.all(
          uncachedAddresses.map(async (address) => {
            return await getTokenMetadataViaHelius(apiKey, address, cacheTtl);
          })
        );
      } else {
        // 大量地址，分批并发处理
        for (let i = 0; i < uncachedAddresses.length; i += concurrency) {
          const batch = uncachedAddresses.slice(i, i + concurrency);
          const batchResults = await Promise.all(
            batch.map(async (address) => {
              return await getTokenMetadataViaHelius(apiKey, address, cacheTtl);
            })
          );
          apiResults.push(...batchResults);
        }
      }
      
      // 过滤掉失败的结果
      apiResults = apiResults.filter(result => result !== null);
    }

    // 合并缓存和 API 结果
    return [...cachedItems, ...apiResults];
  } catch (error) {
    console.error('批量获取代币元数据失败:', error);
    throw error;
  }
}

/**
 * 从 Jupiter API 获取 SOL 的 USD 价格（通过 USDC 报价）
 * @returns {Promise<number|null>} SOL 的 USD 价格
 */
async function getSolPriceFromHelius() {
  // ==================== 配置区域 ====================
  // 请替换为你的 Helius API 密钥
  const HELIUS_API_KEY = config.helius.apiKey;
  // Helius 提供了专门的代币元数据端点
  const apiUrl = `https://mainnet.helius-rpc.com/?api-key=${HELIUS_API_KEY}`;
  // ==================== 配置结束 ====================

  // SOL 的 Mint 地址
  const SOL_MINT = 'So11111111111111111111111111111111111111112';

  console.log(`[1] 📡 正在请求 Helius 代币价格 API...`);
  console.log(`   目标代币: SOL (${SOL_MINT})`);

  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 10000); // 10秒超时

  try {
    // Helius 的 token-metadata 端点需要 POST 请求
    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Accept': 'application/json'
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 'helius-price-query',
        method: 'getAsset', // 获取资产信息
        params: {
          id: SOL_MINT,
          // 显示响应选项 - 可以请求更多详细信息
          displayOptions: {
            showFungible: true,
            showInscription: false
          }
        }
      }),
      signal: controller.signal
    });

    clearTimeout(timeoutId);

    console.log(`[2] ✅ 收到API响应，HTTP状态码: ${response.status}`);

    if (!response.ok) {
      const errorText = await response.text();
      console.error(`[!] ❌ API 响应错误: HTTP ${response.status}`);
      console.error(`   错误详情: ${errorText.substring(0, 200)}`);
      return null;
    }

    const data = await response.json();

    // 调试：查看完整响应结构
    // console.log('完整响应:', JSON.stringify(data, null, 2));

    if (data.error) {
      console.error(`[!] ❌ RPC 错误: ${data.error.message}`);
      return null;
    }

    const assetInfo = data.result;
    console.log(`[3] 📦 成功获取资产信息`);
    console.log(`   代币名称: ${assetInfo.content?.metadata?.name || '未知'}`);
    console.log(`   代币符号: ${assetInfo.content?.metadata?.symbol || '未知'}`);

    // 从 token_info.price_info 获取价格，需要安全检查
    let price = assetInfo?.token_info?.price_info?.price_per_token || null;

    if (price && typeof price === 'number' && price > 0) {
      console.log(`[5] 🎉 成功！SOL 价格: $${price.toFixed(6)}`);
      return price;
    } else {
      console.warn(`[5] ⚠️  获取的价格无效: ${price}`);
      return null;
    }

  } catch (error) {
    clearTimeout(timeoutId);
    console.error(`[!] ❌ 请求失败:`);

    if (error.name === 'AbortError') {
      console.error(`    请求超时 (10秒)`);
    } else {
      console.error(`    错误类型: ${error.name}`);
      console.error(`    错误信息: ${error.message}`);
    }

    return null;
  }
}

/**
 * 获取 SOL 的 USD 价格（使用 Birdeye API，失败则从 CoinGecko 获取，带缓存）
 * @returns {Promise<number|null>} SOL 的 USD 价格
 */
export async function getSolUsdPrice() {
  try {
    const cacheKey = 'sol:usd:price';
    const cacheTtl = 3600;

    // 先尝试从缓存获取
    const cached = await CacheService.get(cacheKey);
    if (cached !== null && cached !== undefined) {
      const price = typeof cached === 'number' ? cached : parseFloat(cached);
      if (!isNaN(price) && price > 0) {
        return price;
      }
    }

    const now = Date.now();

    // 防止并发请求
    if (solPriceFetchPromise && (now - lastFetchTime) < FETCH_COOLDOWN) {
      try {
        return await solPriceFetchPromise;
      } catch (error) {
        console.warn('等待中的价格获取失败，尝试新的获取:', error.message);
      }
    }

    // 创建新的获取 Promise
    solPriceFetchPromise = (async () => {
      lastFetchTime = now;

      try {
        const price = await getSolPriceFromHelius();

        if (price !== null && price > 0) {
          await CacheService.set(cacheKey, price, cacheTtl);
          return price;
        } else {
          // 控制错误日志频率
          const now = Date.now();
          if (now - lastErrorLogTime > ERROR_LOG_COOLDOWN) {
            console.warn('⚠️  从 Helius 获取 SOL 价格返回无效值:', price);
            lastErrorLogTime = now;
          }
          return null;
        }
      } catch (heliusError) {
        console.warn('从 Helius 获取价格失败:', heliusError.message);

        // 更新错误日志时间
        const now = Date.now();
        if (now - lastErrorLogTime > ERROR_LOG_COOLDOWN) {
          console.warn('⚠️  无法从 Helius 获取 SOL 价格');
          lastErrorLogTime = now;
        }

        return null;
      } finally {
        // 清除 Promise 缓存
        solPriceFetchPromise = null;
      }
    })();

    return await solPriceFetchPromise;
  } catch (error) {
    solPriceFetchPromise = null;
    if (!error.message.includes('fetch failed')) {
      console.warn(`获取 SOL 价格失败:`, error.message);
    }
    return null;
  }
}


export default CacheService;


