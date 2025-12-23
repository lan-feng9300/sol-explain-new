import { Connection, PublicKey } from '@solana/web3.js';
import { config } from '../config/index.js';
import { addressFilterConfig } from '../config/addressFilterConfig.js';

// 从配置文件加载过滤列表
const DEX_PROGRAM_IDS = new Set(addressFilterConfig.dexProgramIds);
const KNOWN_CEX_ADDRESSES = new Set(addressFilterConfig.cexAddresses);
const KNOWN_LIQUIDITY_POOL_ADDRESSES = new Set(addressFilterConfig.knownLiquidityPoolAddresses || []);
const SYSTEM_PROGRAMS = new Set(addressFilterConfig.systemPrograms);

/**
 * 检查地址是否是已知的 CEX 地址
 */
export function isKnownCEXAddress(address) {
  return KNOWN_CEX_ADDRESSES.has(address);
}

/**
 * 检查地址是否是 DEX 程序地址
 */
export function isDEXProgramAddress(address) {
  return DEX_PROGRAM_IDS.has(address);
}

/**
 * 检查地址是否是系统程序
 */
export function isSystemOrSpecialProgram(address) {
  return SYSTEM_PROGRAMS.has(address);
}

/**
 * 检查地址是否是已知的流动性池地址（直接过滤，如 Bonk 的流动性池地址）
 */
export function isKnownLiquidityPoolAddress(address) {
  return KNOWN_LIQUIDITY_POOL_ADDRESSES.has(address);
}

/**
 * 通过查询链上数据检查地址是否是流动性池
 * 简化逻辑：只检查账户的 owner 是否是已知的 DEX 程序 ID
 * @param {string} address - 要检查的地址
 * @param {Connection} connection - Solana 连接对象（可选）
 * @returns {Promise<boolean>} 是否是流动性池
 */
export async function isLiquidityPoolAddress(address, connection = null) {
  try {
    // 1. 如果是已知的 CEX 地址，直接过滤
    if (isKnownCEXAddress(address)) {
      return true;
    }

    // 2. 如果是已知的流动性池地址（如 Bonk 的流动性池地址），直接过滤
    if (isKnownLiquidityPoolAddress(address)) {
      return true;
    }

    // 3. 如果是已知的 DEX 程序地址，直接过滤
    if (isDEXProgramAddress(address)) {
      return true;
    }

    // 4. 如果没有提供连接，创建新连接
    if (!connection) {
      connection = new Connection(config.solana.rpcEndpoint, 'confirmed');
    }

    const publicKey = new PublicKey(address);

    // 5. 获取账户信息，检查 owner（带重试机制）
    let accountInfo = null;
    let lastError = null;
    const maxRetries = 3;
    const retryDelay = 1000; // 1秒

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        accountInfo = await Promise.race([
          connection.getAccountInfo(publicKey),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('请求超时')), 10000) // 10秒超时
          )
        ]);
        break; // 成功获取，退出重试循环
      } catch (error) {
        lastError = error;
        if (attempt < maxRetries - 1) {
          // 等待后重试
          await new Promise(resolve => setTimeout(resolve, retryDelay * (attempt + 1)));
          console.warn(`获取账户信息失败，重试 ${attempt + 1}/${maxRetries}:`, error.message);
        }
      }
    }

    if (!accountInfo) {
      // 如果所有重试都失败，记录错误但不阻止处理
      if (lastError) {
        console.warn(`无法获取地址 ${address} 的账户信息（已重试 ${maxRetries} 次）:`, lastError.message);
      }
      return false;
    }

    // 6. 检查账户的 owner（程序所有者）
    const owner = accountInfo.owner.toBase58();

    // 7. 如果 owner 是已知的 DEX 程序 ID，很可能是流动性池（如 Pump.fun 的地址）
    if (isDEXProgramAddress(owner)) {
      return true;
    }

    // 8. 如果 owner 是已知的流动性池地址，也过滤（双重检查）
    if (isKnownLiquidityPoolAddress(owner)) {
      return true;
    }

    return false;
  } catch (error) {
    console.error(`检查地址 ${address} 失败:`, error.message);
    return false; // 出错时返回 false，不阻止处理
  }
}


/**
 * 查询地址的 owner（程序 ID）和账户信息
 * 用于调试和验证地址类型
 * @param {string} address - 要查询的地址
 * @param {Connection} connection - Solana 连接对象（可选）
 * @returns {Promise<Object>} 账户信息
 */
export async function getAddressInfo(address, connection = null) {
  try {
    if (!connection) {
      connection = new Connection(config.solana.rpcEndpoint, 'confirmed');
    }

    const publicKey = new PublicKey(address);
    
    // 添加重试机制和超时控制
    let accountInfo = null;
    const maxRetries = 3;
    const retryDelay = 1000;
    const timeout = 10000;

    for (let attempt = 0; attempt < maxRetries; attempt++) {
      try {
        accountInfo = await Promise.race([
          connection.getAccountInfo(publicKey, { commitment: 'confirmed' }),
          new Promise((_, reject) => 
            setTimeout(() => reject(new Error('请求超时')), timeout)
          )
        ]);
        break;
      } catch (error) {
        if (attempt < maxRetries - 1) {
          const delay = retryDelay * Math.pow(2, attempt);
          await new Promise(resolve => setTimeout(resolve, delay));
          console.warn(`获取地址信息失败，重试 ${attempt + 1}/${maxRetries}:`, error.message);
        } else {
          return {
            address,
            exists: false,
            error: `无法获取账户信息: ${error.message}`
          };
        }
      }
    }

    if (!accountInfo) {
      return {
        address,
        exists: false,
        error: '账户不存在'
      };
    }

    const owner = accountInfo.owner.toBase58();
    const isDEX = isDEXProgramAddress(owner);
    const isCEX = isKnownCEXAddress(address);
    const isKnownPool = isKnownLiquidityPoolAddress(address);
    const isOwnerKnownPool = isKnownLiquidityPoolAddress(owner);

    return {
      address,
      exists: true,
      owner: owner,
      isDEXProgram: isDEX,
      isCEXAddress: isCEX,
      isKnownLiquidityPool: isKnownPool || isOwnerKnownPool,
      isLiquidityPool: isDEX || isKnownPool || isOwnerKnownPool, // 如果 owner 是 DEX 程序或已知流动性池地址，很可能是流动性池
      executable: accountInfo.executable,
      dataLength: accountInfo.data.length,
      lamports: accountInfo.lamports,
      rentEpoch: accountInfo.rentEpoch
    };
  } catch (error) {
    return {
      address,
      exists: false,
      error: error.message
    };
  }
}

/**
 * 获取所有已知的 DEX 程序 ID
 */
export function getDEXProgramIds() {
  return Array.from(DEX_PROGRAM_IDS);
}
