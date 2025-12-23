import { Worker } from 'bullmq';
import { redisConnection, queues } from './queue.js';
import { executeTask } from './taskRegistry.js';

/**
 * Worker 配置
 */
const getWorkerOptions = () => ({
  connection: redisConnection,
  concurrency: 5, // 并发处理任务数
  limiter: {
    max: 10, // 每秒最多处理10个任务
    duration: 1000,
  },
});

/**
 * 创建 Workers
 */
export const workers = {};

/**
 * 初始化 Workers（仅在队列可用时）
 */
export function initWorkers() {
  if (!queues.tokenSync) {
    console.warn('⚠️  队列未初始化，Worker 无法启动');
    return false;
  }

  // 使用任务注册表统一处理所有任务
  workers.tokenSync = new Worker('token-sync', async (job) => {
    const { taskId } = job.data;
    if (taskId) {
      // 如果指定了 taskId，使用任务注册表执行
      return await executeTask(taskId, job);
    } else {
      // 兼容旧的任务格式（直接执行 syncTokenHoldersJob）
      const { syncTokenHoldersJob } = await import('./tasks/tokenSyncTask.js');
      return await syncTokenHoldersJob(job);
    }
  }, getWorkerOptions());
  
  // 初始化事件监听
  initWorkerEvents();
  
  // 检查队列中是否有旧任务
  checkQueueForOldJobs();
  
  return true;
}

/**
 * 检查队列中是否有旧任务（可能是之前运行留下的）
 */
async function checkQueueForOldJobs() {
  try {
    const { queues } = await import('./queue.js');
    if (queues.tokenSync) {
      const [waiting, active] = await Promise.all([
        queues.tokenSync.getWaitingCount(),
        queues.tokenSync.getActiveCount(),
      ]);
      
      if (waiting > 0 || active > 0) {
        console.log(`⚠️  检测到队列中有任务: waiting=${waiting}, active=${active}`);
        console.log(`   这些任务可能是之前运行留下的，workers 会自动处理它们`);
        console.log(`   如果不想处理这些任务，可以清理队列或等待它们完成`);
      } else {
        console.log(`✅ 队列为空，没有待处理的任务`);
      }
    }
  } catch (error) {
    console.error('检查队列状态失败:', error.message);
  }
}

/**
 * 初始化 Worker 事件监听
 */
export function initWorkerEvents() {
  Object.entries(workers).forEach(([name, worker]) => {
    worker.on('completed', (job) => {
      console.log(`[${name}] 任务完成:`, job.id, job.data);
    });

    worker.on('failed', (job, err) => {
      console.error(`[${name}] 任务失败:`, job?.id, err.message);
    });

    worker.on('error', (err) => {
      console.error(`[${name}] Worker 错误:`, err);
    });

    worker.on('active', (job) => {
      console.log(`[${name}] 任务开始执行:`, job.id);
    });
  });
}

/**
 * 关闭所有 Workers
 */
export async function closeWorkers() {
  await Promise.all(
    Object.values(workers).map((worker) => worker.close())
  );
}

