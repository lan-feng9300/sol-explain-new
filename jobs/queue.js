import { Queue, Worker, QueueEvents } from 'bullmq';
import { Redis } from 'ioredis';
import { config } from '../config/index.js';

/**
 * 创建 Redis 连接（带错误处理）
 */
let redisConnection;

try {
  redisConnection = new Redis({
    host: config.redis.HOST,
    port: config.redis.PORT,
    password: config.redis.PASSWORD || undefined,
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    retryStrategy: (times) => {
      if (times > 3) {
        console.error('❌ Redis 连接失败，请确保 Redis 服务正在运行');
        console.error('   安装 Redis: https://redis.io/docs/getting-started/');
        return null; // 停止重试
      }
      return Math.min(times * 200, 2000);
    },
    lazyConnect: true, // 延迟连接，允许检查后再连接
  });

  // 监听连接事件
  redisConnection.on('error', (err) => {
    console.error('❌ Redis 连接错误:', err.message);
    console.error('   请确保 Redis 服务正在运行');
  });

  redisConnection.on('connect', () => {
    console.log('✅ Redis 连接成功');
  });
} catch (error) {
  console.error('❌ 创建 Redis 连接失败:', error.message);
}

/**
 * 队列配置
 */
const queueOptions = {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 3,
    backoff: {
      type: 'exponential',
      delay: 2000,
    },
    removeOnComplete: {
      age: 3600, // 保留1小时
      count: 1000, // 最多保留1000个
    },
    removeOnFail: {
      age: 24 * 3600, // 失败任务保留24小时
    },
  },
};

/**
 * 检查 Redis 是否可用
 */
export async function checkRedisConnection() {
  if (!redisConnection) {
    return false;
  }
  
  try {
    await redisConnection.connect();
    await redisConnection.ping();
    return true;
  } catch (error) {
    console.error('Redis 连接检查失败:', error.message);
    return false;
  }
}

/**
 * 任务队列定义（仅在 Redis 可用时创建）
 */
export const queues = {};

/**
 * 队列事件监听器（用于监控任务状态）
 */
export const queueEvents = {};

/**
 * 初始化队列（仅在 Redis 可用时）
 */
export async function initQueues() {
  const isRedisAvailable = await checkRedisConnection();
  
  if (!isRedisAvailable) {
    console.warn('⚠️  Redis 不可用，任务调度功能将被禁用');
    console.warn('   请安装并启动 Redis 以使用任务调度功能');
    return false;
  }

  // 创建任务队列
  // queues.tokenSync = new Queue('token-sync', queueOptions);
  
  // 创建队列事件监听器
  // queueEvents.tokenSync = new QueueEvents('token-sync', {
  //   connection: redisConnection,
  // });

  return true;
}

/**
 * 初始化队列事件监听
 */
export function initQueueEvents() {
  Object.values(queueEvents).forEach((event) => {
    event.on('completed', ({ jobId }) => {
      console.log(`任务 ${jobId} 完成`);
    });

    event.on('failed', ({ jobId, failedReason }) => {
      console.error(`任务 ${jobId} 失败:`, failedReason);
    });

    event.on('progress', ({ jobId, data }) => {
      console.log(`任务 ${jobId} 进度:`, data);
    });
  });
}

/**
 * 清空所有队列数据
 * 注意：obliterate 方法会直接操作 Redis，删除队列相关的所有键值对
 * BullMQ 的 Queue 对象内部维护了 Redis 连接，所以不需要手动调用 Redis
 */
export async function clearAllQueues() {
  try {
    console.log('正在清空所有队列数据（从 Redis 中删除）...');
    
    const clearPromises = Object.entries(queues).map(async ([name, queue]) => {
      try {
        // 使用 obliterate 方法清空队列（包括 waiting, active, completed, failed 等所有状态的任务）
        // 这个方法会直接调用 Redis 命令，删除队列相关的所有键值对
        // force: true 表示即使有活动任务也强制删除
        const deletedCount = await queue.obliterate({ force: true });
        console.log(`✅ 已清空队列: ${name} (删除了 ${deletedCount || 0} 个任务)`);
        return { success: true, name, deletedCount };
      } catch (error) {
        console.error(`❌ 清空队列 ${name} 失败:`, error.message);
        return { success: false, name, error: error.message };
      }
    });
    
    const results = await Promise.all(clearPromises);
    const successCount = results.filter(r => r.success).length;
    const totalCount = results.length;
    
    if (successCount === totalCount) {
      console.log(`✅ 所有队列数据已清空（${totalCount} 个队列）`);
    } else {
      console.warn(`⚠️  部分队列清空失败: ${successCount}/${totalCount} 成功`);
    }
    
    return results;
  } catch (error) {
    console.error('清空队列时出错:', error.message);
    throw error;
  }
}

/**
 * 关闭所有队列连接
 * @param {boolean} clearData - 是否在关闭前清空队列数据（默认 true）
 */
export async function closeQueues(clearData = true) {
  try {
    // 如果设置了清空数据，先清空所有队列
    if (clearData) {
      await clearAllQueues();
    }
    
    // 关闭队列和事件监听
    await Promise.all([
      ...Object.values(queues).map((queue) => queue.close()),
      ...Object.values(queueEvents).map((event) => event.close()),
    ]);
    
    if (redisConnection) {
      await redisConnection.quit();
    }
    
    console.log('✅ 所有队列连接已关闭');
  } catch (error) {
    console.error('关闭队列连接时出错:', error.message);
  }
}

export { redisConnection };

