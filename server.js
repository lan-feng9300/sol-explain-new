// 加载环境变量（必须在其他导入之前）
import 'dotenv/config';

import express from 'express';
import cors from 'cors';
import tokenRoutes from './routes/tokenRoutes.js';
import jobRoutes from './routes/jobRoutes.js';
import addressMonitorRoutes from './routes/addressMonitorRoutes.js';
import { config } from './config/index.js';
import { initWorkers, closeWorkers } from './jobs/workers.js';
import { initQueueEvents, closeQueues, initQueues } from './jobs/queue.js';
import { initScheduler, stopAllScheduledJobs } from './jobs/scheduler.js';
import { initBullBoard } from './jobs/bullBoard.js';
import { addressMonitorService } from './services/addressMonitorService.js';

const app = express();

// 中间件配置
app.use(cors());
app.use(express.json());
app.use(express.static('public'));

// 路由配置
// 注意：addressMonitorRoutes 必须在 tokenRoutes 之前注册，避免路由冲突
// 因为 /address/connections 是静态路由，必须在 /address/:address 动态路由之前匹配
app.use('/api', addressMonitorRoutes);
app.use('/api', tokenRoutes);
app.use('/api', jobRoutes);

// 任务管理页面
app.get('/jobs', (req, res) => {
  res.sendFile('jobs.html', { root: './public' });
});

// 管理后台页面
app.get('/admin', (req, res) => {
  res.sendFile('admin.html', { root: './public' });
});

// 初始化任务调度系统
async function initJobSystem() {
  try {
    // 初始化队列（会检查 Redis 连接）
    const queuesInitialized = await initQueues();
    
    if (!queuesInitialized) {
      console.log('⚠️  任务调度系统未启用（Redis 不可用）');
      console.log('   请确保 Redis 服务正在运行');
      console.log('   启动 Redis: brew services start redis');
      return;
    }

    // 初始化队列事件监听
    initQueueEvents();
    
    // 初始化 Workers
    const workersInitialized = initWorkers();
    
    if (workersInitialized) {
      // 初始化定时任务（使用队列，默认不自动启动，需要手动启动）
      // 所有定时任务都需要通过管理界面或 API 手动启动，避免项目启动时自动执行
      initScheduler(false);
      console.log('✅ 任务调度系统初始化完成（使用 Redis 队列）');
      console.log('   提示：定时任务需要手动启动，可通过管理界面或 API 启动');
      
      // 初始化 Bull Board（任务队列可视化）
      await initBullBoard(app);
    } else {
      console.log('⚠️  任务调度系统部分初始化（Worker 未启动）');
    }
  } catch (error) {
    console.error('❌ 任务调度系统初始化失败:', error);
  }
}

// 优雅关闭
async function gracefulShutdown() {
  console.log('正在关闭任务调度系统...');
  
  // 停止所有正在运行的定时任务（防止重启后自动执行）
  stopAllScheduledJobs();
  
  // 关闭所有 WebSocket 连接
  console.log('正在关闭所有地址监听连接...');
  addressMonitorService.closeAllConnections();
  
  await closeWorkers();
  await closeQueues();
  process.exit(0);
}

process.on('SIGTERM', gracefulShutdown);
process.on('SIGINT', gracefulShutdown);

// 启动服务器
const PORT = config.server.port;
app.listen(PORT, async () => {
  console.log(`服务器运行在 http://localhost:${PORT}`);
  console.log(`访问 http://localhost:${PORT} 查看前端界面`);
  
  // 初始化任务调度系统
  await initJobSystem();
});

