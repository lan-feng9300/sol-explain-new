import { addJob, getQueueStatus, getScheduledJobs, startScheduledJob, stopScheduledJob } from '../jobs/scheduler.js';
import { queues } from '../jobs/queue.js';
import { getAllTaskDefinitions, getTaskDefinition, validateTaskParams } from '../jobs/taskRegistry.js';

/**
 * 获取所有任务定义列表
 */
export async function getTaskDefinitionsHandler(req, res) {
  try {
    const tasks = getAllTaskDefinitions();
    res.json({
      success: true,
      count: tasks.length,
      tasks,
    });
  } catch (error) {
    console.error('获取任务列表失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * 获取任务定义详情
 */
export async function getTaskDefinitionHandler(req, res) {
  try {
    const { taskId } = req.params;
    const definition = getTaskDefinition(taskId);

    if (!definition) {
      return res.status(404).json({
        success: false,
        error: '任务不存在',
      });
    }

    res.json({
      success: true,
      taskId,
      name: definition.name,
      description: definition.description,
      defaultParams: definition.defaultParams,
      requiredParams: definition.requiredParams,
    });
  } catch (error) {
    console.error('获取任务详情失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * 手动触发任务（使用队列）
 * 支持两种方式：
 * 1. 使用 taskId（推荐）：通过任务注册表执行
 * 2. 兼容旧方式：直接传参数
 */
export async function triggerJobHandler(req, res) {
  try {
    const { taskId, queueName = 'tokenSync', jobName, ...taskParams } = req.body;

    const queue = queues[queueName] || queues.tokenSync;

    if (!queue) {
      return res.status(503).json({
        success: false,
        error: '任务队列不可用，请确保 Redis 服务正在运行',
      });
    }

    let jobData;
    let finalJobName;

    if (taskId) {
      // 使用任务注册表
      const definition = getTaskDefinition(taskId);
      if (!definition) {
        return res.status(404).json({
          success: false,
          error: `任务 ${taskId} 不存在`,
        });
      }

      // 验证参数
      try {
        validateTaskParams(taskId, taskParams);
      } catch (error) {
        return res.status(400).json({
          success: false,
          error: error.message,
        });
      }

      // 合并默认参数和传入参数
      jobData = {
        taskId,
        ...definition.defaultParams,
        ...taskParams,
      };

      finalJobName = jobName || `${definition.name}-${Date.now()}`;
    } else {
      // 兼容旧方式（直接传 tokenAddress）
      const { tokenAddress } = taskParams;
      if (!tokenAddress) {
        return res.status(400).json({
          success: false,
          error: '缺少必要参数: taskId 或 tokenAddress',
        });
      }

      jobData = {
        tokenAddress,
      };

      finalJobName = jobName || `manual-${tokenAddress}-${Date.now()}`;
    }

    const job = await addJob(
      queueName,
      finalJobName,
      jobData,
      {
        jobId: `${finalJobName}-${Date.now()}`,
      }
    );

    res.json({
      success: true,
      message: '任务已添加到队列',
      jobId: job.id,
      jobName: job.name,
      data: job.data,
    });
  } catch (error) {
    console.error('触发任务失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * 获取队列状态
 */
export async function getQueueStatusHandler(req, res) {
  try {
    const { queueName } = req.params;

    if (!queueName) {
      return res.status(400).json({
        success: false,
        error: '缺少队列名称',
      });
    }

    const status = await getQueueStatus(queueName);

    res.json({
      success: true,
      queueName,
      status,
    });
  } catch (error) {
    console.error('获取队列状态失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * 获取任务详情
 */
export async function getJobHandler(req, res) {
  try {
    const { queueName, jobId } = req.params;

    if (!queueName || !jobId) {
      return res.status(400).json({
        success: false,
        error: '缺少必要参数',
      });
    }

    const queue = queues[queueName];
    if (!queue) {
      return res.status(404).json({
        success: false,
        error: '队列不存在',
      });
    }

    const job = await queue.getJob(jobId);
    if (!job) {
      return res.status(404).json({
        success: false,
        error: '任务不存在',
      });
    }

    const state = await job.getState();
    const progress = job.progress;
    const result = job.returnvalue;
    const failedReason = job.failedReason;

    res.json({
      success: true,
      job: {
        id: job.id,
        name: job.name,
        data: job.data,
        state,
        progress,
        result,
        failedReason,
        timestamp: job.timestamp,
        processedOn: job.processedOn,
        finishedOn: job.finishedOn,
      },
    });
  } catch (error) {
    console.error('获取任务详情失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * 获取队列中的所有任务
 */
export async function getJobsHandler(req, res) {
  try {
    const { queueName } = req.params;
    const { status = 'all', limit = 50 } = req.query;

    if (!queueName) {
      return res.status(400).json({
        success: false,
        error: '缺少队列名称',
      });
    }

    const queue = queues[queueName];
    if (!queue) {
      return res.status(404).json({
        success: false,
        error: '队列不存在',
      });
    }

    let jobs = [];
    switch (status) {
      case 'waiting':
        jobs = await queue.getWaiting(0, parseInt(limit));
        break;
      case 'active':
        jobs = await queue.getActive(0, parseInt(limit));
        break;
      case 'completed':
        jobs = await queue.getCompleted(0, parseInt(limit));
        break;
      case 'failed':
        jobs = await queue.getFailed(0, parseInt(limit));
        break;
      default:
        jobs = await queue.getJobs(['waiting', 'active', 'completed', 'failed'], 0, parseInt(limit));
    }

    const jobsData = await Promise.all(
      jobs.map(async (job) => {
        const state = await job.getState();
        return {
          id: job.id,
          name: job.name,
          data: job.data,
          state,
          progress: job.progress,
          timestamp: job.timestamp,
          processedOn: job.processedOn,
          finishedOn: job.finishedOn,
        };
      })
    );

    res.json({
      success: true,
      queueName,
      status,
      count: jobsData.length,
      jobs: jobsData,
    });
  } catch (error) {
    console.error('获取任务列表失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * 获取所有定时任务列表
 */
export async function getScheduledJobsHandler(req, res) {
  try {
    const jobs = getScheduledJobs();
    res.json({
      success: true,
      count: jobs.length,
      jobs,
    });
  } catch (error) {
    console.error('获取定时任务列表失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * 启动定时任务
 */
export async function startScheduledJobHandler(req, res) {
  try {
    const { jobId } = req.params;

    if (!jobId) {
      return res.status(400).json({
        success: false,
        error: '缺少任务 ID',
      });
    }

    const result = startScheduledJob(jobId);
    res.json(result);
  } catch (error) {
    console.error('启动定时任务失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

/**
 * 停止定时任务
 */
export async function stopScheduledJobHandler(req, res) {
  try {
    const { jobId } = req.params;

    if (!jobId) {
      return res.status(400).json({
        success: false,
        error: '缺少任务 ID',
      });
    }

    const result = stopScheduledJob(jobId);
    res.json(result);
  } catch (error) {
    console.error('停止定时任务失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
    });
  }
}

