import { query } from './solAddrSplInfoMapper.js';

/**
 * 保存代币的流动性池地址列表
 * @param {string} tokenAddress - 代币地址
 * @param {Array<string>} poolAddresses - 流动性池地址列表
 */
export async function saveTokenLiquidityPools(tokenAddress, poolAddresses) {
  if (!poolAddresses || poolAddresses.length === 0) {
    return;
  }

  try {
    // 先删除该代币的旧记录（如果有）
    const deleteSql = 'DELETE FROM tbl_token_liquidity_pools WHERE token_address = ?';
    await query(deleteSql, [tokenAddress]);

    // 批量插入新的流动性池地址
    const insertSql = `
      INSERT INTO tbl_token_liquidity_pools (token_address, pool_address, create_at) 
      VALUES (?, ?, NOW())
    `;

    for (const poolAddress of poolAddresses) {
      await query(insertSql, [tokenAddress, poolAddress]);
    }

    console.log(`成功保存 ${poolAddresses.length} 个流动性池地址到数据库 (代币: ${tokenAddress})`);
  } catch (error) {
    console.error('保存流动性池地址失败:', error);
    throw error;
  }
}

/**
 * 获取代币的流动性池地址列表
 * @param {string} tokenAddress - 代币地址
 * @returns {Promise<Array<string>>} 流动性池地址列表
 */
export async function getTokenLiquidityPools(tokenAddress) {
  try {
    const sql = 'SELECT pool_address FROM tbl_token_liquidity_pools WHERE token_address = ?';
    const results = await query(sql, [tokenAddress]);
    return results.map(row => row.pool_address);
  } catch (error) {
    console.error(`获取代币 ${tokenAddress} 的流动性池地址失败:`, error);
    return [];
  }
}

/**
 * 检查代币是否已有流动性池地址记录
 * @param {string} tokenAddress - 代币地址
 * @returns {Promise<boolean>} 是否已有记录
 */
export async function hasTokenLiquidityPools(tokenAddress) {
  try {
    const sql = 'SELECT COUNT(*) as count FROM tbl_token_liquidity_pools WHERE token_address = ? LIMIT 1';
    const results = await query(sql, [tokenAddress]);
    return results[0].count > 0;
  } catch (error) {
    console.error(`检查代币 ${tokenAddress} 的流动性池地址失败:`, error);
    return false;
  }
}

