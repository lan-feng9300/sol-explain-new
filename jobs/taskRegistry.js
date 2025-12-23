/**
 * 任务注册表
 * 类似 xxl-job 的任务管理，每个任务都有唯一的名称和描述
 */

/**
 * 任务定义
 */
export const taskDefinitions = {
  // 代币持有人数据同步任务
  'token-holders-sync': {
    name: '代币持有人数据同步',
    description: '同步代币持有人数据到数据库，分页查询直到持仓百分比小于0.2%，可过滤流动性池和交易所地址',
    handler: async (job) => {
      const { syncTokenHoldersJob } = await import('./tasks/tokenSyncTask.js');
      return await syncTokenHoldersJob(job);
    },
    defaultParams: {
      tokenAddress: '',
      filterPools: true, // 是否过滤流动性池和交易所地址
      checkOnChain: false, // 是否进行链上验证（较慢但更准确）
      isFirstRun: false, // 是否是第一次运行（第一次才进行链上检查，减少 Helius 调用）
    },
    requiredParams: ['tokenAddress'],
  },
  // 可以在这里添加更多任务定义
  // 'another-task': {
  //   name: '另一个任务',
  //   description: '任务描述',
  //   handler: async (job) => { ... },
  //   defaultParams: {},
  //   requiredParams: [],
  // },
};

/**
 * 获取所有任务定义
 */
export function getAllTaskDefinitions() {
  return Object.entries(taskDefinitions).map(([taskId, definition]) => ({
    taskId,
    name: definition.name,
    description: definition.description,
    defaultParams: definition.defaultParams,
    requiredParams: definition.requiredParams,
  }));
}

/**
 * 根据任务ID获取任务定义
 */
export function getTaskDefinition(taskId) {
  return taskDefinitions[taskId];
}

/**
 * 验证任务参数
 */
export function validateTaskParams(taskId, params) {
  const definition = taskDefinitions[taskId];
  if (!definition) {
    throw new Error(`任务 ${taskId} 不存在`);
  }

  // 检查必需参数
  for (const requiredParam of definition.requiredParams) {
    if (!params[requiredParam]) {
      throw new Error(`缺少必需参数: ${requiredParam}`);
    }
  }

  return true;
}

/**
 * 执行任务
 */
export async function executeTask(taskId, job) {
  const definition = taskDefinitions[taskId];
  if (!definition) {
    throw new Error(`任务 ${taskId} 不存在`);
  }

  // 验证参数
  validateTaskParams(taskId, job.data);

  // 执行任务处理器
  return await definition.handler(job);
}

