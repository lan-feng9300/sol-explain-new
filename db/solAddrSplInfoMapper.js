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
 * 批量插入代币持有人信息
 * @param {Array} holders - 持有人数组
 * @param {string} tokenAddress - 代币地址
 * @param {string} tokenSymbol - 代币符号（可选）
 * @param {string} batchTime - 批次时间（格式：2025-11-30 12:05:00）
 */
export async function insertTokenHolders(holders, tokenAddress, tokenSymbol = '', batchTime = null) {
  if (!holders || holders.length === 0) {
    return;
  }

  // 如果没有提供批次时间，使用当前时间（精确到分钟）
  if (!batchTime) {
    const now = new Date();
    const year = now.getFullYear();
    const month = String(now.getMonth() + 1).padStart(2, '0');
    const day = String(now.getDate()).padStart(2, '0');
    const hours = String(now.getHours()).padStart(2, '0');
    const minutes = String(now.getMinutes()).padStart(2, '0');
    batchTime = `${year}-${month}-${day} ${hours}:${minutes}:00`;
  }

  // 使用 INSERT IGNORE 避免重复插入，如果已存在则忽略
  // 如果需要更新已存在的记录，可以使用 REPLACE INTO 或先删除再插入
  // 注意：rank 和 type 是 MySQL 保留关键字，需要用反引号括起来
  const sql = `
    INSERT IGNORE INTO tbl_sol_addr_spl_info 
    (\`addr\`, \`spl_tag\`, \`spl_addr\`, \`rank\`, \`percent\`, \`remark\`, \`type\`, \`batch_time\`) 
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
  `;

  try {
    // 使用事务批量插入
    const connection = await pool.getConnection();
    await connection.beginTransaction();

    try {
      for (const holder of holders) {
        await connection.execute(sql, [
          holder.address,           // addr
          tokenSymbol,              // spl_tag
          tokenAddress,             // spl_addr
          holder.rank || 0,         // rank
          parseFloat(holder.percentage) || 0, // percent
          '',                       // remark
          0,                        // type
          batchTime                 // batch_time
        ]);
      }

      await connection.commit();
      console.log(`成功保存 ${holders.length} 条持有人信息到数据库（批次时间: ${batchTime}）`);
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('保存持有人信息到数据库失败:', error);
    throw error;
  }
}

/**
 * 获取代币持仓占比汇总（按批次时间分组）
 * @param {string} splAddr - 代币地址
 * @returns {Promise<Array>} 批次时间和持仓占比汇总列表
 */
export async function getTokenHoldersSummaryByBatchTime(splAddr) {
  try {
    const sql = `
      SELECT 
        batch_time,
        COUNT(*) as address_count,
        SUM(percent) as total_percent,
        MAX(percent) as max_percent,
        AVG(percent) as avg_percent
      FROM tbl_sol_addr_spl_info
      WHERE spl_addr = ?
      GROUP BY batch_time
      ORDER BY batch_time ASC
    `;
    
    const results = await query(sql, [splAddr]);
    return results;
  } catch (error) {
    console.error('获取代币持仓占比汇总失败:', error);
    throw error;
  }
}

export default pool;

