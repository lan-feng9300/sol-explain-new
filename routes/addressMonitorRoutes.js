import express from 'express';
import {
  subscribeAddressHandler,
  unsubscribeAddressHandler,
  getActiveConnectionsHandler
} from '../controllers/addressMonitorController.js';

const router = express.Router();

/**
 * 订阅地址变化监听（dRPC WebSocket）
 * POST /api/address/:address/subscribe
 * 查询参数:
 *   - encoding: 编码格式，默认为 'jsonParsed'（jsonParsed, base58, base64）
 *   - commitment: 确认级别，默认为 'confirmed'（finalized, confirmed, processed）
 *   - autoCloseAfter: 自动关闭时间（毫秒），0 表示不自动关闭
 * 
 * 示例:
 *   POST /api/address/CM78CPUeXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNH12/subscribe?encoding=jsonParsed&commitment=confirmed
 */
router.post('/address/:address/subscribe', subscribeAddressHandler);

/**
 * 取消订阅地址变化监听
 * POST /api/address/:address/unsubscribe
 * 
 * 示例:
 *   POST /api/address/CM78CPUeXjn8o3yroDHxUtKsZZgoy4GPkPPXfouKNH12/unsubscribe
 */
router.post('/address/:address/unsubscribe', unsubscribeAddressHandler);

/**
 * 获取所有活跃的连接
 * GET /api/address/connections
 * 
 * 示例:
 *   GET /api/address/connections
 */
router.get('/address/connections', getActiveConnectionsHandler);

export default router;

