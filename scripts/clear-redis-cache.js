import Redis from 'ioredis';
import { config } from '../config/index.js';

/**
 * 清除 Redis 中的所有缓存
 */
async function clearRedisCache() {
  let redisClient = null;

  try {
    console.log('正在连接 Redis...');
    redisClient = new Redis({
      host: config.redis.HOST,
      port: config.redis.PORT,
      password: config.redis.PASSWORD || undefined,
      retryStrategy: (times) => {
        if (times > 3) {
          return null;
        }
        return Math.min(times * 50, 2000);
      },
    });

    // 等待连接
    await redisClient.ping();
    console.log('✅ Redis 连接成功');

    // 获取当前数据库的键数量
    const dbSize = await redisClient.dbsize();
    console.log(`当前数据库中有 ${dbSize} 个键`);

    if (dbSize === 0) {
      console.log('数据库已经是空的，无需清除');
      await redisClient.quit();
      return;
    }

    // 询问确认
    console.log('\n⚠️  警告：这将删除 Redis 中的所有数据！');
    console.log('包括：');
    console.log('  - 所有缓存数据');
    console.log('  - 所有任务队列数据');
    console.log('  - 所有流动性池数据');
    console.log('  - 其他所有键值对\n');

    // 清除所有数据
    console.log('正在清除所有数据...');
    await redisClient.flushall();
    console.log('✅ 所有 Redis 数据已清除');

    // 验证清除结果
    const newDbSize = await redisClient.dbsize();
    console.log(`清除后数据库中有 ${newDbSize} 个键`);

    await redisClient.quit();
    console.log('✅ 操作完成');
  } catch (error) {
    console.error('❌ 清除 Redis 缓存失败:', error.message);
    if (redisClient) {
      await redisClient.quit();
    }
    process.exit(1);
  }
}

// 执行清除操作
clearRedisCache();

