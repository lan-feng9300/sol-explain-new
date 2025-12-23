import { addressMonitorService } from '../services/addressMonitorService.js';
import { config } from '../config/index.js';

/**
 * 订阅地址变化监听
 */
export async function subscribeAddressHandler(req, res) {
  try {
    const { address } = req.params;
    const encoding = req.query.encoding || 'jsonParsed';
    const commitment = req.query.commitment || 'confirmed';
    const autoCloseAfter = parseInt(req.query.autoCloseAfter) || 0; // 毫秒，0 表示不自动关闭

    // 参数验证
    if (!address) {
      return res.status(400).json({
        success: false,
        error: '地址不能为空'
      });
    }

    // 检查 API Key
    if (!config.drpc.apiKey) {
      return res.status(400).json({
        success: false,
        error: 'dRPC API Key 未配置',
        hint: '请在 .env 文件中设置 DRPC_API_KEY'
      });
    }

    // 订阅地址变化
    const connectionInfo = await addressMonitorService.subscribeAddress(
      address,
      encoding,
      commitment,
      null, // 回调函数，可以通过其他方式设置
      autoCloseAfter
    );

    res.json({
      success: true,
      message: `已开始监听地址 ${address} 的变化`,
      address,
      encoding,
      commitment,
      subscriptionId: connectionInfo.subscriptionId,
      autoCloseAfter: autoCloseAfter > 0 ? `${autoCloseAfter / 1000} 秒` : '不自动关闭',
      createdAt: connectionInfo.createdAt
    });
  } catch (error) {
    console.error('订阅地址变化监听失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

/**
 * 取消订阅地址变化监听
 */
export async function unsubscribeAddressHandler(req, res) {
  try {
    const { address } = req.params;

    if (!address) {
      return res.status(400).json({
        success: false,
        error: '地址不能为空'
      });
    }

    addressMonitorService.unsubscribeAddress(address);

    res.json({
      success: true,
      message: `已取消监听地址 ${address} 的变化`,
      address
    });
  } catch (error) {
    console.error('取消订阅地址变化监听失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

/**
 * 获取所有活跃的连接
 */
export async function getActiveConnectionsHandler(req, res) {
  try {
    const connections = addressMonitorService.getActiveConnections();

    res.json({
      success: true,
      count: connections.length,
      connections
    });
  } catch (error) {
    console.error('获取活跃连接失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

