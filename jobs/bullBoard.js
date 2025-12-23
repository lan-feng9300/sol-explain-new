import { createBullBoard } from '@bull-board/api';
import { BullMQAdapter } from '@bull-board/api/bullMQAdapter';
import { ExpressAdapter } from '@bull-board/express';
import { queues } from './queue.js';

/**
 * 初始化 Bull Board（任务队列可视化界面）
 * @param {Express} app - Express 应用实例
 */
export async function initBullBoard(app) {
  try {
    // 检查是否有可用的队列
    if (!queues.tokenSync) {
      console.log('⚠️  队列未初始化，Bull Board 无法启动');
      return false;
    }

    const serverAdapter = new ExpressAdapter();
    serverAdapter.setBasePath('/admin/queues');

    createBullBoard({
      queues: [
        new BullMQAdapter(queues.tokenSync),
      ],
      serverAdapter,
    });

    app.use('/admin/queues', serverAdapter.getRouter());

    console.log('✅ Bull Board 已启动');
    console.log('   📊 访问 http://localhost:3000/admin/queues 查看任务队列管理界面');
    return true;
  } catch (error) {
    console.error('❌ Bull Board 初始化失败:', error.message);
    return false;
  }
}

