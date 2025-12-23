import mysql from 'mysql2/promise';
import { config } from '../config/index.js';

/**
 * 创建数据库连接池
 */
const pool = mysql.createPool({
  host: config.mysql.HOST,
  user: config.mysql.USER,
  password: config.mysql.PASSWORD,
  database: config.mysql.DB,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0
});

/**
 * 执行 SQL 查询
 */
export async function query(sql, params) {
  try {
    const [results] = await pool.execute(sql, params);
    return results;
  } catch (error) {
    console.error('数据库查询错误:', error);
    throw error;
  }
}

/**
 * 保存或更新地址盈亏信息
 * @param {string} addr - 钱包地址
 * @param {string} splTag - 代币符号
 * @param {string} splAddr - 代币地址
 * @param {number} pnl - 盈亏（USD）
 * @param totalBuyAmount 总买入金额
 * @param totalSellAmount 总卖出金额
 * @param profileARatio 盈利倍数
 * @param {Object} connection - 数据库连接（可选，用于事务）
 */
export async function checkPnlInfoExist(addr) {
  try {
    const connection = await pool.getConnection();
    await connection.beginTransaction();

    // 先查询是否存在相同的记录
    const checkSql = `
      SELECT distinct addr FROM tbl_sol_addr_pnl_info 
      WHERE addr = ? `;
    const [rows] = connection
        ? await connection.execute(checkSql, [addr])
        : await pool.execute(checkSql, [addr, splAddr]);

    // rows是一个数组，检查是否有查询结果
    return rows.length !== 0;
  } catch (error) {
    console.error('保存地址盈亏信息失败:', error);
    throw error;
  }
}

/**
 * 批量保存地址盈亏信息
 * @param {Array} pnlDataList - 盈亏数据数组，每个元素包含 {
    tokenMint,
    tokenSymbol,
    totalBuyAmount,
    totalSellAmount,
    profit,
    profitRatio
}
 * @param walletAddr
 */
export async function batchSavePnlInfo(pnlDataList, walletAddr) {
  if (!pnlDataList || pnlDataList.length === 0) {
    return;
  }

  try {
    const connection = await pool.getConnection();
    await connection.beginTransaction();

    try {
        const insertSql = `
            INSERT INTO tbl_sol_addr_pnl_info 
                (addr, spl_addr, spl_tag, pnl, total_buy_amount, total_sell_amount, profile_ratio)
            VALUES 
            (?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
            pnl = VALUES(pnl),
            total_buy_amount = VALUES(total_buy_amount),
            total_sell_amount = VALUES(total_sell_amount),
            profile_ratio = VALUES(profile_ratio)
            `;

        // 使用一个循环来执行每条插入，但使用同一个connection，这样可以在同一个事务中
        // 注意：这里我们可以使用prepare语句来优化，但是为了简单，我们直接循环执行
        for (const item of pnlDataList) {
          await connection.execute(insertSql, [
            walletAddr,
            item.tokenMint,
            item.tokenSymbol,
            item.profit,
            item.totalBuyAmount,
            item.totalSellAmount,
            item.profitRatio
          ]);
        }
        await connection.commit();
        console.log(`成功保存 ${pnlDataList.length} 条地址盈亏信息到数据库`);
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('批量保存地址盈亏信息失败:', error);
    throw error;
  }
}

/**
 * 根据钱包地址获取盈亏信息
 * @param {string} addr - 钱包地址
 * @returns {Promise<Array>} 盈亏信息列表
 */
export async function getPnlInfoByAddress(addr) {
  try {
    const sql = `
      SELECT * FROM tbl_sol_addr_pnl_info 
      WHERE addr = ?
      ORDER BY update_at DESC
    `;
    const results = await query(sql, [addr]);
    return results;
  } catch (error) {
    console.error('获取地址盈亏信息失败:', error);
    throw error;
  }
}

/**
 * 根据代币地址获取盈亏信息
 * @param {string} splAddr - 代币地址
 * @returns {Promise<Array>} 盈亏信息列表
 */
export async function getPnlInfoByToken(splAddr) {
  try {
    const sql = `
      SELECT * FROM tbl_sol_addr_pnl_info 
      WHERE spl_addr = ?
      ORDER BY pnl DESC
    `;
    const results = await query(sql, [splAddr]);
    return results;
  } catch (error) {
    console.error('获取代币盈亏信息失败:', error);
    throw error;
  }
}

export default pool;

