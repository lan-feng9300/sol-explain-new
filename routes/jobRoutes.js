import express from 'express';
import {
  triggerJobHandler,
  getQueueStatusHandler,
  getJobHandler,
  getJobsHandler,
  getTaskDefinitionsHandler,
  getTaskDefinitionHandler,
  getScheduledJobsHandler,
  startScheduledJobHandler,
  stopScheduledJobHandler,
} from '../controllers/jobController.js';

const router = express.Router();

/**
 * 获取所有任务定义列表
 * GET /api/jobs/tasks
 */
router.get('/jobs/tasks', getTaskDefinitionsHandler);

/**
 * 获取任务定义详情
 * GET /api/jobs/tasks/:taskId
 */
router.get('/jobs/tasks/:taskId', getTaskDefinitionHandler);

/**
 * 手动触发任务
 * POST /api/jobs/trigger
 * Body: { taskId: 'token-holders-sync', tokenAddress: 'xxx', useHelius: false }
 * 或兼容旧方式: { queueName: 'tokenSync', tokenAddress: 'xxx', useHelius: false }
 */
router.post('/jobs/trigger', triggerJobHandler);

/**
 * 获取队列状态
 * GET /api/jobs/queue/:queueName/status
 */
router.get('/jobs/queue/:queueName/status', getQueueStatusHandler);

/**
 * 获取任务详情
 * GET /api/jobs/queue/:queueName/job/:jobId
 */
router.get('/jobs/queue/:queueName/job/:jobId', getJobHandler);

/**
 * 获取队列中的任务列表
 * GET /api/jobs/queue/:queueName/jobs?status=all&limit=50
 * status: all, waiting, active, completed, failed
 */
router.get('/jobs/queue/:queueName/jobs', getJobsHandler);

/**
 * 获取所有定时任务列表
 * GET /api/jobs/scheduled
 */
router.get('/jobs/scheduled', getScheduledJobsHandler);

/**
 * 启动定时任务
 * POST /api/jobs/scheduled/:jobId/start
 */
router.post('/jobs/scheduled/:jobId/start', startScheduledJobHandler);

/**
 * 停止定时任务
 * POST /api/jobs/scheduled/:jobId/stop
 */
router.post('/jobs/scheduled/:jobId/stop', stopScheduledJobHandler);

export default router;

