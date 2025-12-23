import mysql from 'mysql2/promise';
import {config} from '../config/index.js';

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
 * 保存交易信息
 * @param {string} addr - 钱包地址
 * @param {string} splTag - 代币符号
 * @param {string} splAddr - 代币地址
 * @param {number} type - 交易类型：1-买 2-卖
 * @param {number} solAmount - 交易的 SOL 数量
 * @param {number} splAmount - 交易的代币数量
 * @param {Object} connection - 数据库连接（可选，用于事务）
 * @param {Date|string|number} tradeAt - 交易时间（可选，Unix 时间戳秒或 Date 对象，如果不提供则使用当前时间）
 * @param {string} signature - 交易签名（可选）
 * @param {string} swapFromToken - swap 买入时对应的卖出代币地址（可选）
 * @param {string} swapFromTag - swap 买入时对应的卖出代币符号（可选）
 */
export async function saveTradeInfo(addr, splTag, splAddr, type, solAmount, splAmount, connection = null, tradeAt = null, signature = null, swapFromToken = null, swapFromTag = null) {
  try {
    // 处理交易时间
    let tradeAtValue;
    if (tradeAt) {
      if (typeof tradeAt === 'number') {
        // Unix 时间戳（秒），转换为 MySQL 格式
        const date = new Date(tradeAt * 1000);
        tradeAtValue = date.toISOString().slice(0, 19).replace('T', ' ');
      } else if (tradeAt instanceof Date) {
        tradeAtValue = tradeAt.toISOString().slice(0, 19).replace('T', ' ');
      } else if (typeof tradeAt === 'string') {
        tradeAtValue = tradeAt;
      } else {
        tradeAtValue = 'CURRENT_TIMESTAMP';
      }
    } else {
      tradeAtValue = 'CURRENT_TIMESTAMP';
    }

    // 构建 SQL，根据是否有 signature 和 swap 字段动态调整
    let insertSql;
    let params = [];
    
    // 构建字段列表和占位符列表
    const fields = ['addr', 'spl_tag', 'spl_addr', 'type', 'sol_amount', 'spl_amount'];
    const placeholders = ['?', '?', '?', '?', '?', '?'];
    params.push(addr, splTag, splAddr, type, solAmount, splAmount);
    
    // 添加 trade_at
    fields.push('trade_at');
    if (tradeAtValue === 'CURRENT_TIMESTAMP') {
      placeholders.push('CURRENT_TIMESTAMP');
    } else {
      placeholders.push('?');
      params.push(tradeAtValue);
    }
    
    // 添加 signature（如果有）
    if (signature) {
      fields.push('signature');
      placeholders.push('?');
      params.push(signature);
    }
    
    // 添加 swap_from_token 和 swap_from_tag（如果有）
    if (swapFromToken) {
      fields.push('swap_from_token');
      placeholders.push('?');
      params.push(swapFromToken);
    }
    if (swapFromTag) {
      fields.push('swap_from_tag');
      placeholders.push('?');
      params.push(swapFromTag);
    }
    
    // 构建 SQL
    // 如果 signature 存在，使用 INSERT IGNORE 避免重复插入
    // 注意：需要确保数据库表 signature 字段有唯一索引
    const insertType = signature ? 'INSERT IGNORE' : 'INSERT';
    insertSql = `
      ${insertType} INTO tbl_sol_trade_info 
      (${fields.join(', ')}) 
      VALUES (${placeholders.join(', ')})
    `;
    
    if (connection) {
      await connection.execute(insertSql, params);
    } else {
      await pool.execute(insertSql, params);
    }
  } catch (error) {
    // 如果是唯一索引冲突错误（即使使用了 INSERT IGNORE，某些情况下仍可能抛出错误）
    if (error.code === 'ER_DUP_ENTRY' || error.errno === 1062) {
      // 静默忽略重复插入错误
      return;
    }
    console.error('保存交易信息失败:', error);
    throw error;
  }
}

/**
 * 批量保存交易信息（使用真正的批量插入，避免单条失败影响整批）
 * @param {Array} tradeDataList - 交易数据数组，每个元素包含 {addr, splTag, splAddr, type, solAmount, splAmount, tradeAt, signature, swapFromToken, swapFromTag}
 * tradeAt 可选，可以是 Unix 时间戳（秒）、Date 对象或字符串
 * signature 可选，交易签名
 * swapFromToken 可选，swap 买入时对应的卖出代币地址
 * swapFromTag 可选，swap 买入时对应的卖出代币符号
 */
export async function batchSaveTradeInfo(tradeDataList) {
  if (!tradeDataList || tradeDataList.length === 0) {
    return;
  }

  try {
    const connection = await pool.getConnection();
    await connection.beginTransaction();

    try {
      // 按是否有 signature 分组，因为 signature 有唯一索引
      const withSignature = [];
      const withoutSignature = [];

      tradeDataList.forEach(item => {
        if (item.signature) {
          withSignature.push(item);
        } else {
          withoutSignature.push(item);
        }
      });

      // 批量插入有 signature 的记录（使用 INSERT IGNORE 避免重复）
      if (withSignature.length > 0) {
        await batchInsertWithSignature(connection, withSignature);
      }

      // 批量插入没有 signature 的记录（普通插入）
      if (withoutSignature.length > 0) {
        await batchInsertWithoutSignature(connection, withoutSignature);
      }

      await connection.commit();
      console.log(`成功保存 ${tradeDataList.length} 条交易信息到数据库（有签名: ${withSignature.length}, 无签名: ${withoutSignature.length}）`);
    } catch (error) {
      await connection.rollback();
      throw error;
    } finally {
      connection.release();
    }
  } catch (error) {
    console.error('批量保存交易信息失败:', error);
    throw error;
  }
}

/**
 * 批量插入有 signature 的记录（使用 INSERT IGNORE）
 */
async function batchInsertWithSignature(connection, tradeDataList) {
  if (tradeDataList.length === 0) return;

  // 构建批量插入 SQL
  const fields = ['addr', 'spl_tag', 'spl_addr', 'type', 'sol_amount', 'spl_amount', 'trade_at', 'signature'];
  const allFields = [...fields];
  
  // 检查是否有 swap 字段
  const hasSwapFields = tradeDataList.some(item => item.swapFromToken || item.swapFromTag);
  if (hasSwapFields) {
    allFields.push('swap_from_token', 'swap_from_tag');
  }

  const values = [];
  const params = [];

  tradeDataList.forEach(item => {
    const rowValues = [];
    
    // addr
    rowValues.push('?');
    params.push(item.addr);
    
    // spl_tag
    rowValues.push('?');
    params.push(item.splTag);
    
    // spl_addr
    rowValues.push('?');
    params.push(item.splAddr);
    
    // type
    rowValues.push('?');
    params.push(item.type);
    
    // sol_amount
    rowValues.push('?');
    params.push(item.solAmount);
    
    // spl_amount
    rowValues.push('?');
    params.push(item.splAmount);
    
    // trade_at
    if (item.tradeAt) {
      if (typeof item.tradeAt === 'number') {
        const date = new Date(item.tradeAt * 1000);
        rowValues.push('?');
        params.push(date.toISOString().slice(0, 19).replace('T', ' '));
      } else if (item.tradeAt instanceof Date) {
        rowValues.push('?');
        params.push(item.tradeAt.toISOString().slice(0, 19).replace('T', ' '));
      } else {
        rowValues.push('?');
        params.push(item.tradeAt);
      }
    } else {
      rowValues.push('CURRENT_TIMESTAMP');
    }
    
    // signature
    rowValues.push('?');
    params.push(item.signature);
    
    // swap_from_token 和 swap_from_tag
    if (hasSwapFields) {
      rowValues.push('?');
      params.push(item.swapFromToken || null);
      rowValues.push('?');
      params.push(item.swapFromTag || null);
    }
    
    values.push(`(${rowValues.join(', ')})`);
  });

  const sql = `
    INSERT IGNORE INTO tbl_sol_trade_info 
    (${allFields.join(', ')}) 
    VALUES ${values.join(', ')}
  `;

  await connection.execute(sql, params);
}

/**
 * 批量插入没有 signature 的记录（普通插入）
 */
async function batchInsertWithoutSignature(connection, tradeDataList) {
  if (tradeDataList.length === 0) return;

  // 构建批量插入 SQL
  const fields = ['addr', 'spl_tag', 'spl_addr', 'type', 'sol_amount', 'spl_amount', 'trade_at'];
  const allFields = [...fields];
  
  // 检查是否有 swap 字段
  const hasSwapFields = tradeDataList.some(item => item.swapFromToken || item.swapFromTag);
  if (hasSwapFields) {
    allFields.push('swap_from_token', 'swap_from_tag');
  }

  const values = [];
  const params = [];

  tradeDataList.forEach(item => {
    const rowValues = [];
    
    // addr
    rowValues.push('?');
    params.push(item.addr);
    
    // spl_tag
    rowValues.push('?');
    params.push(item.splTag);
    
    // spl_addr
    rowValues.push('?');
    params.push(item.splAddr);
    
    // type
    rowValues.push('?');
    params.push(item.type);
    
    // sol_amount
    rowValues.push('?');
    params.push(item.solAmount);
    
    // spl_amount
    rowValues.push('?');
    params.push(item.splAmount);
    
    // trade_at
    if (item.tradeAt) {
      if (typeof item.tradeAt === 'number') {
        const date = new Date(item.tradeAt * 1000);
        rowValues.push('?');
        params.push(date.toISOString().slice(0, 19).replace('T', ' '));
      } else if (item.tradeAt instanceof Date) {
        rowValues.push('?');
        params.push(item.tradeAt.toISOString().slice(0, 19).replace('T', ' '));
      } else {
        rowValues.push('?');
        params.push(item.tradeAt);
      }
    } else {
      rowValues.push('CURRENT_TIMESTAMP');
    }
    
    // swap_from_token 和 swap_from_tag
    if (hasSwapFields) {
      rowValues.push('?');
      params.push(item.swapFromToken || null);
      rowValues.push('?');
      params.push(item.swapFromTag || null);
    }
    
    values.push(`(${rowValues.join(', ')})`);
  });

  const sql = `
    INSERT INTO tbl_sol_trade_info 
    (${allFields.join(', ')}) 
    VALUES ${values.join(', ')}
  `;

  await connection.execute(sql, params);
}

/**
 * 根据钱包地址获取交易信息
 * @param {string} addr - 钱包地址
 * @param {number} limit - 限制返回数量
 * @returns {Promise<Array>} 交易信息列表
 */
export async function getTradeInfoByAddress(addr, limit = 100) {
  try {
    const sql = `
      SELECT * FROM tbl_sol_trade_info 
      WHERE addr = ?
      ORDER BY trade_at DESC
      LIMIT ?
    `;
    const results = await query(sql, [addr, limit]);
    return results;
  } catch (error) {
    console.error('获取地址交易信息失败:', error);
    throw error;
  }
}

/**
 * 根据代币地址获取交易信息
 * @param {string} splAddr - 代币地址
 * @param {number} limit - 限制返回数量
 * @returns {Promise<Array>} 交易信息列表
 */
export async function getTradeInfoByToken(splAddr, limit = 100) {
  try {
    const sql = `
      SELECT * FROM tbl_sol_trade_info 
      WHERE spl_addr = ?
      ORDER BY trade_at DESC
      LIMIT ?
    `;
    const results = await query(sql, [splAddr, limit]);
    return results;
  } catch (error) {
    console.error('获取代币交易信息失败:', error);
    throw error;
  }
}

/**
 * 获取所有机器人地址列表
 * @returns {Promise<Set<string>>} 机器人地址集合（已规范化：去除空格，转小写）
 */
export async function getRobotAddresses() {
  try {
    const sql = `
      SELECT DISTINCT addr FROM tbl_sol_addr_type
    `;
    const results = await query(sql, []);
    // 规范化地址：去除首尾空格，转小写（Solana 地址不区分大小写）
    const robotAddresses = new Set(
      results
        .map(row => (row.addr || '').trim().toLowerCase())
        .filter(addr => addr.length > 0)
    );
    console.log(`✓ 获取到 ${robotAddresses.size} 个机器人地址`);
    return robotAddresses;
  } catch (error) {
    console.error('获取机器人地址列表失败:', error);
    // 如果表不存在或查询失败，返回空集合，不进行过滤
    return new Set();
  }
}

/**
 * 获取归总后的代币和地址（用于分析持有者盈亏）
 * 按代币地址和钱包地址分组，统计总数量，过滤出数量大于指定值的记录
 * @param {number} minAmount - 最小数量阈值（默认 1500000）
 * @returns {Promise<Array>} 归总后的记录列表，包含 spl_addr, addr, num
 */
export async function getGroupedTokenAddresses(minAmount = 1500000) {
  try {
    const sql = `
      SELECT t.* FROM (
        SELECT spl_addr, addr, sum(spl_amount) as num 
        FROM tbl_sol_trade_info 
        GROUP BY spl_addr, addr
      ) t 
      WHERE t.num > ?
      ORDER BY t.num DESC
    `;
    return await query(sql, [minAmount]);
  } catch (error) {
    console.error('获取归总后的代币和地址失败:', error);
    throw error;
  }
}

export async function saveAndMarkAddr(addr) {
  try {
    const connection = await pool.getConnection();
    await connection.beginTransaction();

    const insertSql = `
      INSERT INTO tbl_sol_addr_type
      (addr, type) VALUES (?, ?)
      ON DUPLICATE KEY UPDATE
      addr = VALUES(addr)
    `;

    await connection.execute(insertSql, [addr, 2]);
    await connection.commit();
  } catch (error) {
    console.error('保存标记地址失败:', error);
    throw error;
  }
}

export async function getZhiShouAddr(walletAddressList) {
  try {
    if (!walletAddressList || walletAddressList.length === 0) {
      return [];
    }

    // 构建动态数量的占位符
    const placeholders = walletAddressList.map(() => '?').join(',');

    const sql = `
      SELECT DISTINCT addr
      FROM tbl_sol_addr_type
      WHERE addr IN (${placeholders})
    `;

    return await query(sql, walletAddressList);
  } catch (error) {
    console.error('获取纸手地址失败:', error);
    throw error;
  }
}

export default pool;

