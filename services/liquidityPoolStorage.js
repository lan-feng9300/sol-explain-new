import { getTokenLiquidityPools, saveTokenLiquidityPools, hasTokenLiquidityPools } from '../db/liquidityPoolMapper.js';
import Redis from 'ioredis';
import { config } from '../config/index.js';

let redisClient = null;

/**
 * 初始化 Redis 客户端
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

    return redisClient;
  } catch (error) {
    console.warn('Redis 连接失败，将使用数据库存储:', error.message);
    return null;
  }
}

/**
 * 获取 Redis Key
 */
function getRedisKey(tokenAddress) {
  return `token:liquidity-pools:${tokenAddress}`;
}

/**
 * 保存代币的流动性池地址列表（优先使用 Redis，失败则使用数据库）
 * @param {string} tokenAddress - 代币地址
 * @param {Array<string>} poolAddresses - 流动性池地址列表
 */
export async function saveLiquidityPools(tokenAddress, poolAddresses) {
  if (!poolAddresses || poolAddresses.length === 0) {
    return;
  }

  // 优先使用 Redis
  try {
    const client = initRedisClient();
    if (client) {
      const key = getRedisKey(tokenAddress);
      await client.setex(key, 86400 * 30, JSON.stringify(poolAddresses)); // 30天过期
      console.log(`已保存 ${poolAddresses.length} 个流动性池地址到 Redis (代币: ${tokenAddress})`);
      return;
    }
  } catch (error) {
    console.warn('保存到 Redis 失败，使用数据库:', error.message);
  }

  // 使用数据库作为备选
  await saveTokenLiquidityPools(tokenAddress, poolAddresses);
}

/**
 * 获取代币的流动性池地址列表（优先使用 Redis，失败则使用数据库）
 * @param {string} tokenAddress - 代币地址
 * @returns {Promise<Array<string>>} 流动性池地址列表
 */
export async function getLiquidityPools(tokenAddress) {
  // 优先使用 Redis
  try {
    const client = initRedisClient();
    if (client) {
      const key = getRedisKey(tokenAddress);
      const data = await client.get(key);
      if (data) {
        return JSON.parse(data);
      }
    }
  } catch (error) {
    console.warn('从 Redis 获取失败，使用数据库:', error.message);
  }

  // 使用数据库作为备选
  return await getTokenLiquidityPools(tokenAddress);
}

/**
 * 检查代币是否已有流动性池地址记录
 * @param {string} tokenAddress - 代币地址
 * @returns {Promise<boolean>} 是否已有记录
 */
export async function hasLiquidityPools(tokenAddress) {
  // 优先使用 Redis
  try {
    const client = initRedisClient();
    if (client) {
      const key = getRedisKey(tokenAddress);
      const exists = await client.exists(key);
      if (exists === 1) {
        return true;
      }
    }
  } catch (error) {
    console.warn('检查 Redis 失败，使用数据库:', error.message);
  }

  // 使用数据库作为备选
  return await hasTokenLiquidityPools(tokenAddress);
}

