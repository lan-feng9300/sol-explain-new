import { batchSavePnlInfo } from '../db/solAddrPnlInfoMapper.js';

/**
 * 保存钱包盈亏详情到数据库
 * @param {string} walletAddress - 钱包地址
 * @param {Object} birdeyeResponse - Birdeye API 返回的完整响应
 * @returns {Promise<Object>} 保存结果
 */
export async function saveWalletPnlToDatabase(walletAddress, birdeyeResponse) {
  try {
    // 解析 Birdeye API 返回的数据结构
    const tokens = birdeyeResponse?.data?.tokens || [];
    
    if (!tokens || tokens.length === 0) {
      console.log(`钱包 ${walletAddress} 没有代币盈亏数据需要保存`);
      return {
        success: true,
        savedCount: 0,
        message: '没有代币盈亏数据'
      };
    }

    // 构建要保存的数据列表
    const pnlDataList = tokens.map(token => ({
      addr: walletAddress,
      splTag: token.symbol || '',
      splAddr: token.address || '',
      pnl: token.pnl?.total_usd || 0
    }));

    // 批量保存到数据库
    await batchSavePnlInfo(pnlDataList);

    return {
      success: true,
      savedCount: pnlDataList.length,
      message: `成功保存 ${pnlDataList.length} 条盈亏信息`
    };
  } catch (error) {
    console.error('保存钱包盈亏详情到数据库失败:', error);
    throw error;
  }
}

