import { queues } from './queue.js';
import cron from 'node-cron';

/**
 * 定时任务配置
 * 格式: 分 时 日 月 周
 * 
 * 示例：
 * '0 0 * * *' - 每天午夜执行
 * 
 * 重要提示：
 * - 所有定时任务默认不自动启动，需要通过管理界面或 API 手动启动
 * - 新增定时任务时，请确保遵循这个模式（不自动启动）
 * - 启动方式：通过 /api/jobs/scheduled/:jobId/start API 或管理界面启动
 */
const scheduledJobsConfig = [
  {
    id: 'sync-token-holders',
    name: '同步热门代币持有人数据',
    schedule: '*/5 * * * *', // 每5分钟执行一次（分 时 日 月 周）
    queue: 'tokenSync',
    data: {
      // 可以配置要同步的代币地址列表
      tokenAddresses: [
        'E7geR74zbneUJFYSNrZQppsX2TVbi84G8aXw5dpCbonk' // SOL
      ],
    },
  },
];

// 存储 cron 任务实例，用于启动/停止
const cronTasks = new Map();

/**
 * 初始化定时任务
 * @param {boolean} autoStart - 是否自动启动（默认 false，所有 job 需要手动启动）
 * 
 * 注意：所有定时任务默认不自动启动，需要通过管理界面或 API 手动启动
 * 这样可以避免项目启动时自动执行任务，给用户更多控制权
 */
export function initScheduler(autoStart = false) {
  console.log('初始化定时任务调度器...');
  console.log(`定时任务模式: ${autoStart ? '自动启动' : '手动启动（需要通过管理界面或 API 启动）'}`);

  scheduledJobsConfig.forEach((jobConfig) => {
    if (jobConfig.data.tokenAddresses && jobConfig.data.tokenAddresses.length > 0) {
      // 创建任务函数
      const taskFunction = async () => {
          const startTime = Date.now();
          console.log(`[${new Date().toISOString()}] 执行定时任务: ${jobConfig.name}`);
          
          try {
            // 为每个代币地址创建任务（使用任务注册表）
            // 使用 Promise.allSettled 并行添加任务，提高效率
            const addJobPromises = jobConfig.data.tokenAddresses.map(async (tokenAddress) => {
              try {
                await queues[jobConfig.queue].add(
                  `${jobConfig.name}-${tokenAddress}`,
                  {
                    taskId: 'token-holders-sync', // 使用任务注册表的 taskId
                    tokenAddress,
                    filterPools: true,
                    // isFirstRun 将在任务内部自动判断，不需要在这里检查
                  },
                  {
                    jobId: `scheduled-${tokenAddress}-${Date.now()}`,
                  }
                );
                console.log(`已添加任务: ${tokenAddress}`);
                return { success: true, tokenAddress };
              } catch (error) {
                console.error(`添加任务失败 ${tokenAddress}:`, error);
                return { success: false, tokenAddress, error: error.message };
              }
            });

            const results = await Promise.allSettled(addJobPromises);
            const duration = Date.now() - startTime;
            console.log(`[${new Date().toISOString()}] 定时任务完成: ${jobConfig.name} (耗时: ${duration}ms)`);
          } catch (error) {
            console.error(`定时任务执行失败: ${jobConfig.name}`, error);
          }
      };

      // 创建 cron 任务（但不自动启动）
      const cronTask = cron.schedule(
        jobConfig.schedule,
        taskFunction,
        {
          scheduled: autoStart, // 根据参数决定是否自动启动
          timezone: 'Asia/Shanghai', // 设置时区
        }
      );
      
      // 存储任务实例
      cronTasks.set(jobConfig.id, {
        id: jobConfig.id,
        name: jobConfig.name,
        schedule: jobConfig.schedule,
        queue: jobConfig.queue,
        data: jobConfig.data,
        task: cronTask,
        running: autoStart,
      });
      
      // 详细日志输出，确认任务状态
      if (autoStart) {
        console.log(`⚠️  已注册并自动启动定时任务: ${jobConfig.name} (${jobConfig.schedule})`);
      } else {
        console.log(`✅ 已注册定时任务: ${jobConfig.name} (${jobConfig.schedule}) - 状态: 未启动（需要手动启动）`);
        console.log(`   启动方式: 通过管理界面或 API: POST /api/jobs/scheduled/${jobConfig.id}/start`);
      }
    } else {
      console.log(`跳过定时任务: ${jobConfig.name} (未配置代币地址)`);
    }
  });
}

/**
 * 获取所有定时任务列表
 */
export function getScheduledJobs() {
  return Array.from(cronTasks.values()).map(job => ({
    id: job.id,
    name: job.name,
    schedule: job.schedule,
    queue: job.queue,
    data: job.data,
    running: job.running,
  }));
}

/**
 * 启动定时任务
 * @param {string} jobId - 任务 ID
 */
export function startScheduledJob(jobId) {
  const job = cronTasks.get(jobId);
  if (!job) {
    throw new Error(`定时任务 ${jobId} 不存在`);
  }

  if (job.running) {
    throw new Error(`定时任务 ${jobId} 已经在运行中`);
  }

  job.task.start();
  job.running = true;
  console.log(`已启动定时任务: ${job.name}`);
  return { success: true, message: `定时任务 ${job.name} 已启动` };
}

/**
 * 停止定时任务
 * @param {string} jobId - 任务 ID
 */
export function stopScheduledJob(jobId) {
  const job = cronTasks.get(jobId);
  if (!job) {
    throw new Error(`定时任务 ${jobId} 不存在`);
  }

  if (!job.running) {
    throw new Error(`定时任务 ${jobId} 已经停止`);
  }

  job.task.stop();
  job.running = false;
  console.log(`已停止定时任务: ${job.name}`);
  return { success: true, message: `定时任务 ${job.name} 已停止` };
}

/**
 * 停止所有正在运行的定时任务
 * 用于服务器关闭时清理
 */
export function stopAllScheduledJobs() {
  console.log('正在停止所有定时任务...');
  let stoppedCount = 0;
  
  cronTasks.forEach((job, jobId) => {
    if (job.running) {
      try {
        job.task.stop();
        job.running = false;
        stoppedCount++;
        console.log(`已停止定时任务: ${job.name}`);
      } catch (error) {
        console.error(`停止定时任务 ${job.name} 失败:`, error.message);
      }
    }
  });
  
  if (stoppedCount > 0) {
    console.log(`✅ 已停止 ${stoppedCount} 个定时任务`);
  } else {
    console.log(`✅ 没有正在运行的定时任务需要停止`);
  }
  
  return stoppedCount;
}

/**
 * 手动添加任务到队列
 * @param {string} queueName - 队列名称
 * @param {string} jobName - 任务名称
 * @param {Object} data - 任务数据
 * @param {Object} options - 任务选项
 */
export async function addJob(queueName, jobName, data, options = {}) {
  const queue = queues[queueName];
  if (!queue) {
    throw new Error(`队列 ${queueName} 不存在。请确保 Redis 服务正在运行。`);
  }

  const job = await queue.add(jobName, data, {
    jobId: options.jobId || `${jobName}-${Date.now()}`,
    ...options,
  });

  return job;
}

/**
 * 获取队列状态
 * @param {string} queueName - 队列名称
 */
export async function getQueueStatus(queueName) {
  const queue = queues[queueName];
  if (!queue) {
    throw new Error(`队列 ${queueName} 不存在`);
  }

  const [waiting, active, completed, failed] = await Promise.all([
    queue.getWaitingCount(),
    queue.getActiveCount(),
    queue.getCompletedCount(),
    queue.getFailedCount(),
  ]);

  return {
    waiting,
    active,
    completed,
    failed,
    total: waiting + active + completed + failed,
  };
}

