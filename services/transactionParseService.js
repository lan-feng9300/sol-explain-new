import { Connection } from '@solana/web3.js';
import { config } from '../config/index.js';
import SolanaDexTradeParser from './SolanaDexTradeParser.js';

// SOL 代币地址常量
const SOL_MINT = 'So11111111111111111111111111111111111111112';

// 常见代币 symbol 映射表
const TOKEN_SYMBOL_MAP = {
  // USDC (Solana)
  'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v': 'USDC',
  // USDC (Wormhole)
  'FkimKUQhh72rJKxSD6awD7KUdf6yYwhz2weBrRgvSYbX': 'USDC',
  // USDT (Solana)
  'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB': 'USDT',
  // WSOL (Wrapped SOL)
  'So11111111111111111111111111111111111111112': 'SOL',
};

/**
 * 检查是否是 SOL 代币
 */
function isSOL(mint) {
  return mint === SOL_MINT || mint === 'SOL';
}

/**
 * 获取代币符号
 * @param {string} mint - 代币 mint 地址
 * @param {string} defaultSymbol - 默认 symbol（从解析器获取的）
 * @returns {string} 代币 symbol
 */
function getTokenSymbol(mint, defaultSymbol) {
  if (!mint) {
    return defaultSymbol || 'Unknown';
  }
  
  // 检查是否是 SOL
  if (isSOL(mint)) {
    return 'SOL';
  }
  
  // 检查映射表
  if (TOKEN_SYMBOL_MAP[mint]) {
    return TOKEN_SYMBOL_MAP[mint];
  }
  
  // 如果 defaultSymbol 不是 'Token' 或 'Unknown'，使用它
  if (defaultSymbol && defaultSymbol !== 'Token' && defaultSymbol !== 'Unknown') {
    return defaultSymbol;
  }
  
  // 否则返回 'Unknown'
  return 'Unknown';
}

// 复用 Connection 和 Parser 实例（单例模式）
let cachedConnection = null;
let cachedParser = null;

/**
 * 获取 Connection 实例（单例）
 */
function getConnection() {
  if (!cachedConnection) {
    const rpcEndpoint = config.drpc.apiKey
      ? `https://lb.drpc.live/solana/${config.drpc.apiKey}`
      : config.solana.rpcEndpoint;
    cachedConnection = new Connection(rpcEndpoint, 'confirmed');
  }
  return cachedConnection;
}

/**
 * 获取 Parser 实例（单例）
 */
function getParser() {
  if (!cachedParser) {
    cachedParser = new SolanaDexTradeParser(getConnection());
  }
  return cachedParser;
}

/**
 * 检查 RPC 配置是否可用
 */
export function checkRPCConfig() {
  return !!(config.drpc.apiKey || config.helius.apiKey);
}

/**
 * 生成错误调试信息
 */
async function generateErrorDebugInfo(signature, error) {
  try {
    const connection = getConnection();
    const transaction = await connection.getParsedTransaction(signature, {
      commitment: 'confirmed',
      maxSupportedTransactionVersion: 0
    });

    if (transaction) {
      const programIds = transaction.transaction?.message?.instructions
        ?.map(ix => ix.programId?.toString()) || [];
      const tokenBalances = transaction.meta?.postTokenBalances?.length || 0;
      const solBalanceChanges = transaction.meta?.postBalances?.length || 0;

      return {
        transactionFound: true,
        programIds: programIds.slice(0, 10),
        tokenBalanceAccounts: tokenBalances,
        solBalanceAccounts: solBalanceChanges,
        error: error.message
      };
    } else {
      return {
        transactionFound: false,
        error: '交易不存在或无法获取'
      };
    }
  } catch (debugError) {
    console.error('获取调试信息失败:', debugError);
    return { error: debugError.message };
  }
}

/**
 * 分析交易余额变化（用于调试）
 */
function analyzeTransactionChanges(transaction, parser) {
  const meta = transaction.meta || {};
  const preTokenBalances = meta.preTokenBalances || [];
  const postTokenBalances = meta.postTokenBalances || [];
  const preBalances = meta.preBalances || [];
  const postBalances = meta.postBalances || [];

  // 分析代币变化
  const tokenChanges = [];
  const tokenBalanceMap = new Map();

  // 先收集所有 post token balances
  postTokenBalances.forEach(post => {
    const key = `${post.owner}-${post.mint}`;
    tokenBalanceMap.set(key, {
      mint: post.mint,
      owner: post.owner,
      postAmount: post.uiTokenAmount?.uiAmount || 0,
      decimals: post.uiTokenAmount?.decimals || 0
    });
  });

  // 然后匹配 pre token balances
  preTokenBalances.forEach(pre => {
    const key = `${pre.owner}-${pre.mint}`;
    const post = tokenBalanceMap.get(key);
    if (post) {
      const preAmount = pre.uiTokenAmount?.uiAmount || 0;
      const change = post.postAmount - preAmount;
      if (Math.abs(change) > 0.000001) {
        tokenChanges.push({
          mint: post.mint,
          owner: post.owner,
          change: change,
          decimals: post.decimals
        });
      }
    }
  });

  // 分析 SOL 变化
  const solChanges = [];
  postBalances.forEach((postBalance, index) => {
    const preBalance = preBalances[index] || 0;
    const change = (postBalance - preBalance) / 1e9;
    if (Math.abs(change) > 0.001) {
      const account = transaction.transaction?.message?.accountKeys?.[index]?.pubkey?.toString() || `account_${index}`;
      solChanges.push({
        account: account,
        change: change
      });
    }
  });

  const dexType = parser.identifyDEX(transaction);

  return {
    transactionFound: true,
    dexType: dexType,
    programIds: transaction.transaction?.message?.instructions
      ?.map(ix => ix.programId?.toString()).slice(0, 10) || [],
    tokenChanges: tokenChanges.length,
    solChanges: solChanges.length,
    tokenChangesDetails: tokenChanges.slice(0, 5).map(tc => ({
      mint: tc.mint?.substring(0, 8) + '...',
      change: tc.change,
      owner: tc.owner?.substring(0, 8) + '...'
    })),
    solChangesDetails: solChanges.slice(0, 5).map(sc => ({
      account: sc.account?.substring(0, 8) + '...',
      change: sc.change
    })),
    analysis: {
      hasTokenChanges: tokenChanges.length > 0,
      hasSolChanges: solChanges.length > 0,
      isSimpleTrade: tokenChanges.length === 1 && solChanges.length === 1,
      reason: tokenChanges.length === 0 && solChanges.length === 0
        ? '交易没有明显的代币或SOL余额变化'
        : tokenChanges.length > 1 || solChanges.length > 1
        ? '交易包含多个代币或SOL变化，当前解析器仅支持简单的买卖模式'
        : '交易模式不符合解析器的预期'
    }
  };
}

/**
 * 生成未找到交易时的调试信息
 */
async function generateNotFoundDebugInfo(signature) {
  try {
    const connection = getConnection();
    const parser = getParser();
    const transaction = await connection.getParsedTransaction(signature, {
      commitment: 'confirmed',
      maxSupportedTransactionVersion: 0
    });

    if (transaction) {
      return analyzeTransactionChanges(transaction, parser);
    } else {
      return {
        transactionFound: false,
        error: '交易不存在或无法获取'
      };
    }
  } catch (debugError) {
    console.error('获取调试信息失败:', debugError);
    return { error: debugError.message };
  }
}

/**
 * 格式化交易解析结果
 */
function formatTradeResult(tradeInfo, signature) {
  return {
    signature: tradeInfo.signature || signature,
    transactionType: tradeInfo.type, // 'buy', 'sell', 'swap'
    dex: tradeInfo.dex, // 'jupiter', 'raydium', 'orca', 'pump_fun_amm', 'unknown'
    source: tradeInfo.source, // 数据来源
    timestamp: tradeInfo.timestamp,
    slot: tradeInfo.slot,
    
    // 买卖信息
    buySellInfo: {
      type: tradeInfo.type,
      source: tradeInfo.source,
      soldToken: {
        mint: tradeInfo.soldToken?.mint,
        symbol: getTokenSymbol(tradeInfo.soldToken?.mint, tradeInfo.soldToken?.symbol),
        amount: tradeInfo.soldToken?.amount,
        decimals: tradeInfo.soldToken?.decimals
      },
      boughtToken: {
        mint: tradeInfo.boughtToken?.mint,
        symbol: getTokenSymbol(tradeInfo.boughtToken?.mint, tradeInfo.boughtToken?.symbol),
        amount: tradeInfo.boughtToken?.amount,
        decimals: tradeInfo.boughtToken?.decimals
      },
      price: tradeInfo.price,
      fee: tradeInfo.fee || null,
      holderAddress: tradeInfo.holderAddress || null
    },
    
    // 额外信息（如果有）
    route: tradeInfo.route || null,
    platform: tradeInfo.platform || null,
    
    // 调试信息（如果有）
    debug: tradeInfo._debug || null
  };
}

/**
 * 轻量级解析交易买卖信息（用于批量处理）
 * @param {string} signature - 交易签名
 * @returns {Promise<object|null>} 解析结果，如果无法解析返回 null
 */
async function parseTradeInfoInternal(signature) {
  const parser = getParser();

  try {
    // 解析交易（使用缓存，快速失败）
    const tradeInfo = await parser.parseTrade(signature, {
      commitment: 'confirmed',
      maxSupportedTransactionVersion: 0,
      useJupiterAPI: false, // 批量处理时禁用 Jupiter API，避免超时
      useCache: true
    });

    if (!tradeInfo) {
      return null;
    }

    // 返回简化的买卖信息
    return {
      type: tradeInfo.type, // 'buy', 'sell', 'swap'
      dex: tradeInfo.dex,
      source: tradeInfo.source,
      soldToken: {
        mint: tradeInfo.soldToken?.mint,
        symbol: getTokenSymbol(tradeInfo.soldToken?.mint, tradeInfo.soldToken?.symbol),
        amount: tradeInfo.soldToken?.amount,
        decimals: tradeInfo.soldToken?.decimals
      },
      boughtToken: {
        mint: tradeInfo.boughtToken?.mint,
        symbol: getTokenSymbol(tradeInfo.boughtToken?.mint, tradeInfo.boughtToken?.symbol),
        amount: tradeInfo.boughtToken?.amount,
        decimals: tradeInfo.boughtToken?.decimals
      },
      price: tradeInfo.price,
      fee: tradeInfo.fee || null
    };
  } catch (error) {
    // 批量处理时静默失败，不记录错误
    return null;
  }
}

// 导出 parseTradeInfo 函数
export const parseTradeInfo = parseTradeInfoInternal;

/**
 * 批量解析交易买卖信息（优化版：始终使用批量 RPC 调用，大幅提升性能）
 * @param {string[]} signatures - 交易签名数组
 * @param {number} concurrency - 并发数量（用于批次大小，默认 50）
 * @returns {Promise<Map<string, object>>} 签名到买卖信息的映射
 */
export async function parseMultipleTradeInfo(signatures, concurrency = 50) {
  const results = new Map();
  const parser = getParser();
  const connection = getConnection();
  
  if (signatures.length === 0) {
    return results;
  }
  
  console.log(`开始批量解析 ${signatures.length} 笔交易（使用批量 RPC 调用）...`);
  const startTime = Date.now();
  
  // 对于少量数据（≤50），一次性批量获取所有交易
  // 对于大量数据，分批处理，每批最多 50 个（Solana RPC 推荐批量大小）
  const batchSize = signatures.length <= 50 ? signatures.length : Math.min(concurrency, 50);
  
  for (let i = 0; i < signatures.length; i += batchSize) {
    const batch = signatures.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(signatures.length / batchSize);
    
    try {
      // 关键优化：一次性批量获取整批交易数据（1 次 RPC 调用 vs N 次单独调用）
      const batchStartTime = Date.now();
      const transactions = await connection.getParsedTransactions(batch, {
        commitment: 'confirmed',
        maxSupportedTransactionVersion: 0
      });
      
      const fetchTime = Date.now() - batchStartTime;
      console.log(`批次 ${batchNumber}/${totalBatches}: 批量获取 ${batch.length} 个交易耗时 ${fetchTime}ms`);
      
      if (!transactions || transactions.length === 0) {
        console.warn(`批次 ${batchNumber} 未获取到交易数据`);
        continue;
      }
      
      // 同步解析每笔交易（CPU 密集型，但很快，不需要 async）
      const parseStartTime = Date.now();
      const batchResults = batch.map((signature, index) => {
        try {
          const transaction = transactions[index];
          if (!transaction) {
            return { signature, tradeInfo: null };
          }
          
          // 使用已获取的交易数据直接解析（同步操作，非常快）
          const tradeInfo = parseTradeFromTransactionSync(parser, signature, transaction);
          return { signature, tradeInfo };
        } catch (error) {
          // 静默失败
          return { signature, tradeInfo: null };
        }
      });
      
      const parseTime = Date.now() - parseStartTime;
      
      // 处理结果
      let successCount = 0;
      batchResults.forEach((result) => {
        if (result.tradeInfo) {
          results.set(result.signature, result.tradeInfo);
          successCount++;
        }
      });
      
      console.log(`批次 ${batchNumber}/${totalBatches}: 解析完成 (${successCount}/${batch.length} 成功, 解析耗时 ${parseTime}ms)`);
    } catch (error) {
      console.error(`批次 ${batchNumber} 批量处理失败:`, error.message);
      // 批量失败时，尝试单个获取（作为最后手段）
      console.log(`回退到单个获取模式...`);
      const fallbackResults = await Promise.allSettled(
      batch.map(async (signature) => {
          try {
            const tradeInfo = await parseTradeInfoInternal(signature);
        return { signature, tradeInfo };
          } catch (e) {
            return { signature, tradeInfo: null };
          }
        })
      );
      
      fallbackResults.forEach((result) => {
        if (result.status === 'fulfilled') {
          const { signature, tradeInfo } = result.value;
          if (tradeInfo) {
            results.set(signature, tradeInfo);
          }
        }
      });
    }
  }
  
  const totalTime = Date.now() - startTime;
  console.log(`✓ 批量解析完成: ${results.size}/${signatures.length} 个成功，总耗时 ${totalTime}ms (平均 ${(totalTime / signatures.length).toFixed(0)}ms/笔)`);
  
  return results;
}

/**
 * 从已获取的交易数据解析买卖信息（同步版本，避免不必要的 async/await 开销）
 * @param {object} parser - SolanaDexTradeParser 实例
 * @param {string} signature - 交易签名
 * @param {object} transaction - 已获取的交易数据
 * @returns {object|null} 解析结果
 */
function parseTradeFromTransactionSync(parser, signature, transaction) {

  try {
    if (!transaction) {
      return null;
    }
    
    // 检查交易数据格式
    if (!transaction.transaction || !transaction.meta) {
      return null;
    }
    
    // 识别 DEX 类型并解析（同步操作，很快）
    const dexType = parser.identifyDEX(transaction);

    let tradeInfo;
    switch (dexType) {
      case 'jupiter':
        tradeInfo = parser.parseJupiterTransaction(transaction);
        break;
      case 'raydium':
        tradeInfo = parser.parseRaydiumTransaction(transaction);
        break;
      case 'orca':
        tradeInfo = parser.parseOrcaTransaction(transaction);
        break;
      case 'pump_fun':
        tradeInfo = parser.parsePumpFunTransaction(transaction);
        break;
      case 'meteora':
      case 'dflow':
        // Meteora DLMM 和 DFlow Aggregator 使用通用解析器
        // 通用解析器会通过代币余额变化来解析交易
        tradeInfo = parser.parseGenericTransaction(transaction, dexType === 'meteora' ? 'Meteora DLMM' : 'DFlow Aggregator');
        break;
      default:
        tradeInfo = parser.parseGenericTransaction(transaction, 'unknown');
    }
    
    if (tradeInfo) {
      tradeInfo.signature = signature;
      tradeInfo.timestamp = transaction.blockTime ? new Date(transaction.blockTime * 1000) : null;
      tradeInfo.slot = transaction.slot;
      
      // 返回简化的买卖信息
      const result = {
        type: tradeInfo.type,
        dex: tradeInfo.dex,
        source: tradeInfo.source,
        soldToken: {
          mint: tradeInfo.soldToken?.mint,
          symbol: getTokenSymbol(tradeInfo.soldToken?.mint, tradeInfo.soldToken?.symbol),
          amount: tradeInfo.soldToken?.amount,
          decimals: tradeInfo.soldToken?.decimals
        },
        boughtToken: {
          mint: tradeInfo.boughtToken?.mint,
          symbol: getTokenSymbol(tradeInfo.boughtToken?.mint, tradeInfo.boughtToken?.symbol),
          amount: tradeInfo.boughtToken?.amount,
          decimals: tradeInfo.boughtToken?.decimals
        },
        price: tradeInfo.price,
        fee: tradeInfo.fee || null,
        holderAddress: tradeInfo.holderAddress || null
      };

      return result;
    }

    return null;
  } catch (error) {
    if (isDebugSignature) {
      console.error(`\n[解析调试] 签名 ${signature}: 解析出错`, error);
    }
    // 静默失败，不记录错误（批量处理时）
    return null;
  }
}

/**
 * 从已有的交易数据批量解析买卖信息（无需 RPC 调用，性能最优）
 * @param {Array} transactions - 已包含 transaction 和 meta 的交易数据数组
 * @returns {Promise<Map<string, object>>} 签名到买卖信息的映射
 */
export async function parseMultipleTradeInfoFromTransactions(transactions) {
  const results = new Map();
  const parser = getParser();
  
  if (!transactions || transactions.length === 0) {
    return results;
  }
  
  console.log(`开始从已有交易数据解析 ${transactions.length} 笔交易的买卖信息（无需 RPC 调用）...`);
  const startTime = Date.now();
  
  // 直接使用已有的交易数据解析（同步操作，非常快）
  transactions.forEach((txData, index) => {
    try {
      // 获取签名（可能在不同位置）
      const signature = txData.signature || txData.transaction?.signatures?.[0];
      if (!signature) {
        return;
      }

      // 检查是否有完整的交易数据
      if (!txData.transaction || !txData.meta) {
        return;
      }
      
      // 构建标准的 parsed transaction 格式
      const transaction = {
        transaction: txData.transaction,
        meta: txData.meta,
        slot: txData.slot,
        blockTime: txData.blockTime,
        version: txData.version
      };
      
      // 使用同步解析函数
      const tradeInfo = parseTradeFromTransactionSync(parser, signature, transaction);

      // todo 有签名放这里去打印给cursor调试解析
      // const DEBUG_SIGNATURES = [
      //   '有签名放这里去打印给cursor调试解析'
      // ];
      // if (signature && DEBUG_SIGNATURES.includes(signature)) {
      //   console.log(`\n[parseMultipleTradeInfoFromTransactions] 调试签名 ${signature} 的解析结果:`);
      //   if (tradeInfo) {
      //     console.log(JSON.stringify(tradeInfo, null, 2));
      //   } else {
      //     console.log('解析结果: null (解析失败)');
      //   }
      // }
      
      if (tradeInfo) {
        results.set(signature, tradeInfo);
      }
    } catch (error) {
      // 静默失败
    }
  });
  
  const totalTime = Date.now() - startTime;
  console.log(`✓ 从已有数据解析完成: ${results.size}/${transactions.length} 个成功，总耗时 ${totalTime}ms (平均 ${(totalTime / transactions.length).toFixed(0)}ms/笔)`);
  
  return results;
}

/**
 * 解析交易的买入/卖出信息
 * @param {string} signature - 交易签名
 * @returns {Promise<object>} 解析结果
 */
export async function parseTransactionBuySellInfo(signature) {
  const connection = getConnection();
  const parser = getParser();

  try {
    // 解析交易
    const tradeInfo = await parser.parseTrade(signature, {
      commitment: 'confirmed',
      maxSupportedTransactionVersion: 0,
      useJupiterAPI: true,
      useCache: true
    });

    if (!tradeInfo) {
      // 如果返回 null，生成调试信息
      const debugInfo = await generateNotFoundDebugInfo(signature);
      return {
        success: false,
        error: '无法解析交易信息',
        hint: '交易可能不是标准的 DEX 交易，或交易模式不符合解析器的预期',
        debug: debugInfo,
        suggestions: [
          '检查交易是否包含代币余额变化',
          '检查交易是否涉及 SOL 和代币的交换',
          '交易可能包含多个代币变化，当前解析器仅支持简单的买卖模式',
          '可以访问 Solscan 查看交易的详细信息'
        ]
      };
    }

    // 格式化返回结果
    const result = formatTradeResult(tradeInfo, signature);
    return {
      success: true,
      signature: signature,
      data: result
    };

  } catch (parseError) {
    console.error('解析交易时出错:', parseError);
    
    // 生成错误调试信息
    const debugInfo = await generateErrorDebugInfo(signature, parseError);
    
    return {
      success: false,
      error: '解析交易时出错',
      message: parseError.message,
      debug: debugInfo,
      hint: '请检查交易签名是否正确，或交易是否为 DEX 交易'
    };
  }
}

