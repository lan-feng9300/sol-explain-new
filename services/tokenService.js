import { Connection, PublicKey } from '@solana/web3.js';
import { 
  getTokenTransactionsViaHelius, 
  parseHeliusTransaction, 
  getTokenTransactionsViaSolscan 
} from '../src/tokenTracker.js';
import { config } from '../config/index.js';

/**
 * 获取代币交易记录
 */
export async function getTokenTransactions(tokenAddress, limit = 100, useHelius = false) {
  let transactions = [];
  
  if (useHelius && config.helius.apiKey) {
    // 使用 Helius API（推荐）
    const rawTransactions = await getTokenTransactionsViaHelius(
      config.helius.apiKey,
      tokenAddress,
      limit
    );
    
    // 解析交易数据
    transactions = rawTransactions
      .map(tx => parseHeliusTransaction(tx, tokenAddress))
      .filter(tx => tx !== null);
  } else {
    // 尝试使用 Solscan API（免费，无需 API Key）
    const rawTransactions = await getTokenTransactionsViaSolscan(tokenAddress, limit);
    
    // 解析交易数据
    transactions = rawTransactions
      .map(tx => parseHeliusTransaction(tx, tokenAddress))
      .filter(tx => tx !== null);
  }
  
  return transactions;
}

/**
 * 计算交易统计信息
 */
export function calculateTransactionStats(transactions) {
  return {
    total: transactions.length,
    buys: transactions.filter(tx => tx.type === 'buy').length,
    sells: transactions.filter(tx => tx.type === 'sell').length,
    totalVolume: transactions.reduce((sum, tx) => sum + (tx.solAmount || 0), 0)
  };
}

/**
 * 获取交易对交易记录
 */
export async function getPoolTransactions(poolAddress, limit = 100) {
  const connection = new Connection(config.solana.rpcEndpoint, 'confirmed');
  const poolPublicKey = new PublicKey(poolAddress);
  
  // 获取交易签名
  const signatures = await connection.getSignaturesForAddress(
    poolPublicKey,
    { limit }
  );
  
  // 获取交易详情
  const transactions = await Promise.all(
    signatures.map(async (sig) => {
      try {
        const tx = await connection.getTransaction(sig.signature, {
          maxSupportedTransactionVersion: 0
        });
        return {
          signature: sig.signature,
          blockTime: sig.blockTime,
          slot: sig.slot,
          err: sig.err,
          transaction: tx
        };
      } catch (error) {
        console.error(`获取交易 ${sig.signature} 失败:`, error);
        return null;
      }
    })
  );
  
  return transactions.filter(tx => tx !== null);
}

/**
 * 获取地址交易记录（基础版本）
 */
export async function getAddressTransactions(address, limit = 50) {
  const connection = new Connection(config.solana.rpcEndpoint, 'confirmed');
  const publicKey = new PublicKey(address);
  
  // 获取该地址的所有交易签名
  const signatures = await connection.getSignaturesForAddress(
    publicKey,
    { limit }
  );
  
  // 获取交易详情
  const transactions = await Promise.all(
    signatures.map(async (sig) => {
      const tx = await connection.getTransaction(sig.signature, {
        maxSupportedTransactionVersion: 0
      });
      return {
        signature: sig.signature,
        blockTime: sig.blockTime,
        slot: sig.slot,
        err: sig.err,
        transaction: tx
      };
    })
  );
  
  return transactions;
}

/**
 * 延迟函数（用于避免速率限制）
 */
function delay(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

/**
 * 从指令中解析 swap 信息（优先方法）
 * 检查是否有 swap wsol for token（买入）或 swap token for wsol（卖出）
 * 
 * @param {Object} parsedTx - getParsedTransaction 返回的解析后交易对象
 * @returns {Object|null} swap 信息，包含类型和详情
 */
function parseSwapFromInstructions(parsedTx) {
  if (!parsedTx || !parsedTx.transaction || !parsedTx.transaction.message) {
    return null;
  }

  const instructions = parsedTx.transaction.message.instructions || [];
  const meta = parsedTx.meta || {};
  const logMessages = meta.logMessages || [];
  
  // WSOL (Wrapped SOL) 的 mint 地址
  const WSOL_MINT = 'So11111111111111111111111111111111111111112';
  
  // 查找 swap 相关的指令
  for (const instruction of instructions) {
    // 检查是否是解析后的指令
    if (instruction.parsed) {
      const parsed = instruction.parsed;
      
      // 检查是否是 swap 指令（不同 DEX 可能有不同的格式）
      // Raydium, Orca, Jupiter 等 DEX 的 swap 指令
      if (parsed.type === 'swap' || 
          parsed.type === 'swapBaseIn' || 
          parsed.type === 'swapBaseOut' ||
          (parsed.info && (parsed.info.instruction === 'swap' || parsed.info.instruction === 'swapBaseIn' || parsed.info.instruction === 'swapBaseOut'))) {
        
        // 尝试从指令中提取代币信息
        const info = parsed.info || parsed;
        
        // 检查是否有 sourceMint 和 destinationMint
        if (info.sourceMint || info.destinationMint || info.mint || info.tokenMint) {
          const sourceMint = info.sourceMint || info.mint || info.tokenMint;
          const destinationMint = info.destinationMint || info.mint || info.tokenMint;
          
          // 判断 swap 方向
          if (sourceMint === WSOL_MINT && destinationMint && destinationMint !== WSOL_MINT) {
            // swap wsol for token = 买入
            return {
              type: 'buy',
              description: `Swap WSOL for Token (买入)`,
              sourceMint: sourceMint,
              destinationMint: destinationMint,
              method: 'instruction_swap'
            };
          } else if (sourceMint && sourceMint !== WSOL_MINT && destinationMint === WSOL_MINT) {
            // swap token for wsol = 卖出
            return {
              type: 'sell',
              description: `Swap Token for WSOL (卖出)`,
              sourceMint: sourceMint,
              destinationMint: destinationMint,
              method: 'instruction_swap'
            };
          }
        }
      }
    }
    
  }
  
  // 方法2：从日志消息中解析 swap 信息（在循环外处理，避免重复检查）
  // 很多 DEX 会在日志中记录 swap 信息，格式类似 "Program log: swap wsol for token"
  if (logMessages && logMessages.length > 0) {
    for (const log of logMessages) {
      const logLower = log.toLowerCase();
      
      // 查找 swap 相关的日志
      if (logLower.includes('swap')) {
        // 检查是否是 swap wsol for token（买入）
        // 格式可能是 "swap wsol for xxx" 或 "swap sol for xxx"
        if ((logLower.includes('wsol') || logLower.includes('sol')) && logLower.includes('for')) {
          const forIndex = logLower.indexOf('for');
          if (forIndex > -1) {
            const beforeFor = logLower.substring(0, forIndex).trim();
            const afterFor = logLower.substring(forIndex + 3).trim();
            
            // 如果 "for" 前面是 wsol/sol，后面不是 wsol/sol，说明是用 wsol 换取代币（买入）
            if ((beforeFor.includes('wsol') || beforeFor.includes('sol')) && 
                !afterFor.startsWith('wsol') && !afterFor.startsWith('sol') && afterFor.length > 0) {
              return {
                type: 'buy',
                description: `Swap WSOL for Token (买入) - 从日志解析`,
                method: 'log_message_swap',
                logMessage: log
              };
            }
          }
        }
        
        // 检查是否是 swap token for wsol（卖出）
        // 格式可能是 "swap xxx for wsol" 或 "swap xxx for sol"
        if (logLower.includes('for') && (logLower.includes('wsol') || logLower.includes('sol'))) {
          const forIndex = logLower.indexOf('for');
          if (forIndex > -1) {
            const beforeFor = logLower.substring(0, forIndex).trim();
            const afterFor = logLower.substring(forIndex + 3).trim();
            
            // 如果 "for" 后面是 wsol/sol，且前面不是 wsol/sol，说明是用代币换取 wsol（卖出）
            if ((afterFor.startsWith('wsol') || afterFor.startsWith('sol')) && 
                !beforeFor.includes('wsol') && !beforeFor.includes('sol') && beforeFor.length > 0) {
              return {
                type: 'sell',
                description: `Swap Token for WSOL (卖出) - 从日志解析`,
                method: 'log_message_swap',
                logMessage: log
              };
            }
          }
        }
      }
    }
  }
  
  return null;
}

/**
 * 判断交易类型（买入/卖出）
 * 从 getParsedTransaction 返回的数据中判断交易是买入还是卖出
 * 
 * 判断逻辑（优先级从高到低）：
 * 1. 从 swap 指令中解析（最准确）：swap wsol for token = 买入，swap token for wsol = 卖出
 * 2. 从余额变化判断：SOL 减少 + 代币增加 = 买入，SOL 增加 + 代币减少 = 卖出
 * 3. 转账：只有代币转移，没有 SOL 转移
 * 4. 未知：无法判断
 * 
 * @param {Object} parsedTx - getParsedTransaction 返回的解析后交易对象
 * @param {Array} accountKeys - 账户公钥列表
 * @param {Array} preBalances - 交易前 SOL 余额（lamports）
 * @param {Array} postBalances - 交易后 SOL 余额（lamports）
 * @param {Array} preTokenBalances - 交易前代币余额
 * @param {Array} postTokenBalances - 交易后代币余额
 * @returns {Object} 交易类型信息
 */
function determineTransactionType(parsedTx, accountKeys, preBalances, postBalances, preTokenBalances, postTokenBalances) {
  // 优先方法：从 swap 指令/日志中解析
  const swapInfo = parseSwapFromInstructions(parsedTx);
  if (swapInfo) {
    // 如果从 swap 指令中解析到了信息，还需要结合余额变化来计算具体数量
    // 但类型已经确定，可以直接返回
    return {
      type: swapInfo.type,
      description: swapInfo.description,
      method: swapInfo.method,
      sourceMint: swapInfo.sourceMint,
      destinationMint: swapInfo.destinationMint,
      amountIn: swapInfo.amountIn,
      amountOut: swapInfo.amountOut,
      logMessage: swapInfo.logMessage,
      solChanges: [],
      tokenChanges: [],
      totalSolChange: 0,
      totalSolAbsChange: 0,
      details: {
        tokenMint: swapInfo.destinationMint || swapInfo.sourceMint,
        method: 'swap_instruction',
        amountIn: swapInfo.amountIn,
        amountOut: swapInfo.amountOut
      },
      _raw: {
        method: 'swap_instruction_parsing',
        swapInfo: swapInfo
      }
    };
  }
  try {
    // 1. 计算 SOL 余额变化
    const solChanges = [];
    if (preBalances && postBalances && accountKeys) {
      for (let i = 0; i < Math.min(preBalances.length, postBalances.length); i++) {
        const preSol = preBalances[i] / 1e9; // 转换为 SOL
        const postSol = postBalances[i] / 1e9;
        const solChange = postSol - preSol;
        
        // 忽略微小变化（可能是交易费用）
        if (Math.abs(solChange) > 0.0001) {
          solChanges.push({
            accountIndex: i,
            address: accountKeys[i]?.pubkey?.toString() || accountKeys[i]?.toString() || `Account${i}`,
            change: solChange,
            preBalance: preSol,
            postBalance: postSol
          });
        }
      }
    }

    // 2. 计算代币余额变化（查找所有代币）
    const tokenChanges = [];
    if (preTokenBalances && postTokenBalances) {
      // 创建代币余额映射
      const tokenBalanceMap = new Map();
      
      // 处理交易前的代币余额
      preTokenBalances.forEach(balance => {
        const key = `${balance.accountIndex}-${balance.mint}`;
        tokenBalanceMap.set(key, {
          accountIndex: balance.accountIndex,
          mint: balance.mint,
          owner: balance.owner,
          preAmount: parseFloat(balance.uiTokenAmount?.uiAmountString || '0'),
          postAmount: 0
        });
      });
      
      // 处理交易后的代币余额
      postTokenBalances.forEach(balance => {
        const key = `${balance.accountIndex}-${balance.mint}`;
        const existing = tokenBalanceMap.get(key);
        if (existing) {
          existing.postAmount = parseFloat(balance.uiTokenAmount?.uiAmountString || '0');
        } else {
          tokenBalanceMap.set(key, {
            accountIndex: balance.accountIndex,
            mint: balance.mint,
            owner: balance.owner,
            preAmount: 0,
            postAmount: parseFloat(balance.uiTokenAmount?.uiAmountString || '0')
          });
        }
      });
      
      // 计算变化
      tokenBalanceMap.forEach((balance, key) => {
        const change = balance.postAmount - balance.preAmount;
        if (Math.abs(change) > 0.00000001) { // 忽略微小变化
          tokenChanges.push({
            ...balance,
            change: change,
            address: accountKeys[balance.accountIndex]?.pubkey?.toString() || accountKeys[balance.accountIndex]?.toString() || `Account${balance.accountIndex}`
          });
        }
      });
    }

    // 3. 计算总变化
    // totalSolChange: 净变化（正数表示增加，负数表示减少）
    const totalSolChange = solChanges.reduce((sum, change) => sum + change.change, 0);
    // totalSolAbsChange: 绝对值总和（用于计算总交易量）
    const totalSolAbsChange = solChanges.reduce((sum, change) => sum + Math.abs(change.change), 0);
    
    // 计算实际用于交易的 SOL 金额（排除手续费和找零）
    // 对于买入：只计算 SOL 减少的部分
    // 对于卖出：只计算 SOL 增加的部分
    let actualSolAmount = 0;
    if (totalSolChange < 0) {
      // SOL 净减少（买入），计算减少的总量
      actualSolAmount = Math.abs(totalSolChange);
    } else if (totalSolChange > 0) {
      // SOL 净增加（卖出），计算增加的总量
      actualSolAmount = totalSolChange;
    } else {
      // 净变化为0，使用绝对值（可能是双向转账）
      actualSolAmount = totalSolAbsChange;
    }
    
    // 按代币 mint 分组计算
    const tokenChangesByMint = new Map();
    tokenChanges.forEach(change => {
      if (!tokenChangesByMint.has(change.mint)) {
        tokenChangesByMint.set(change.mint, []);
      }
      tokenChangesByMint.get(change.mint).push(change);
    });

    // 4. 判断交易类型
    let type = 'unknown';
    let description = '无法判断交易类型';
    let details = {};

    // 如果有代币变化和 SOL 变化
    if (tokenChanges.length > 0 && totalSolAbsChange > 0.001) {
      // 遍历每个代币 mint
      tokenChangesByMint.forEach((changes, mint) => {
        const totalTokenChange = changes.reduce((sum, c) => sum + c.change, 0);
        
        // 买入：代币增加，SOL 减少（注意：totalSolChange 是净变化，买入时应该是负数）
        if (totalTokenChange > 0 && totalSolChange < -0.001) {
          // 使用实际花费的 SOL（只计算减少的部分），而不是净变化
          // 这样可以排除手续费、找零等
          const buySolAmount = actualSolSpent > 0 ? actualSolSpent : Math.abs(totalSolChange);
          type = 'buy';
          description = `买入交易：获得 ${Math.abs(totalTokenChange).toFixed(4)} 个代币，花费 ${buySolAmount.toFixed(4)} SOL`;
          details = {
            tokenMint: mint,
            tokenAmount: totalTokenChange,
            solAmount: buySolAmount, // 实际花费的 SOL（排除手续费和找零）
            price: buySolAmount / totalTokenChange, // SOL per token
            direction: 'in' // 代币流入
          };
        } 
        // 卖出：代币减少，SOL 增加
        else if (totalTokenChange < 0 && totalSolChange > 0.001) {
          // 使用实际获得的 SOL（只计算增加的部分），而不是净变化
          const sellSolAmount = actualSolReceived > 0 ? actualSolReceived : totalSolChange;
          type = 'sell';
          description = `卖出交易：卖出 ${Math.abs(totalTokenChange).toFixed(4)} 个代币，获得 ${sellSolAmount.toFixed(4)} SOL`;
          details = {
            tokenMint: mint,
            tokenAmount: Math.abs(totalTokenChange),
            solAmount: sellSolAmount, // 实际获得的 SOL（排除手续费）
            price: sellSolAmount / Math.abs(totalTokenChange), // SOL per token
            direction: 'out' // 代币流出
          };
        }
        // 如果代币和 SOL 都增加或都减少，可能是特殊情况，尝试用绝对值判断
        else if (Math.abs(totalTokenChange) > 0.00000001 && totalSolAbsChange > 0.001) {
          // 如果代币增加但 SOL 也增加（可能是特殊情况），或者代币减少但 SOL 也减少
          // 这种情况下，我们根据代币变化的方向来判断
          if (totalTokenChange > 0) {
            // 代币增加，即使 SOL 也增加，也认为是买入（可能是复合交易）
            type = 'buy';
            description = `买入交易：获得 ${Math.abs(totalTokenChange).toFixed(4)} 个代币，涉及 ${totalSolAbsChange.toFixed(4)} SOL`;
            details = {
              tokenMint: mint,
              tokenAmount: totalTokenChange,
              solAmount: totalSolAbsChange,
              price: totalSolAbsChange / totalTokenChange,
              direction: 'in'
            };
          } else if (totalTokenChange < 0) {
            // 代币减少，即使 SOL 也减少，也认为是卖出（可能是复合交易）
            type = 'sell';
            description = `卖出交易：卖出 ${Math.abs(totalTokenChange).toFixed(4)} 个代币，涉及 ${totalSolAbsChange.toFixed(4)} SOL`;
            details = {
              tokenMint: mint,
              tokenAmount: Math.abs(totalTokenChange),
              solAmount: totalSolAbsChange,
              price: totalSolAbsChange / Math.abs(totalTokenChange),
              direction: 'out'
            };
          }
        }
      });
    } 
    // 如果只有代币变化，没有 SOL 变化（可能是转账）
    else if (tokenChanges.length > 0 && totalSolAbsChange <= 0.001) {
      type = 'transfer';
      description = '代币转账：只有代币转移，没有 SOL 转移';
      details = {
        tokenTransfers: tokenChanges,
        solAmount: 0
      };
    }
    // 如果只有 SOL 变化，没有代币变化（可能是纯 SOL 转账或未知交易）
    else if (tokenChanges.length === 0 && totalSolAbsChange > 0.001) {
      // 纯 SOL 转账，无法判断是买入还是卖出
      type = 'unknown';
      description = `SOL 转账：涉及 ${totalSolAbsChange.toFixed(4)} SOL，但没有代币变化`;
      details = {
        solAmount: totalSolAbsChange,
        solChanges: solChanges
      };
    }

    return {
      type: type,
      description: description,
      solChanges: solChanges,
      tokenChanges: tokenChanges,
      totalSolChange: totalSolChange,
      totalSolAbsChange: totalSolAbsChange,
      details: details,
      // 原始数据（用于调试）
      _raw: {
        solChangeCount: solChanges.length,
        tokenChangeCount: tokenChanges.length,
        tokenMints: Array.from(tokenChangesByMint.keys())
      }
    };
  } catch (error) {
    console.error('判断交易类型失败:', error);
    return {
      type: 'unknown',
      description: `判断失败: ${error.message}`,
      error: error.message
    };
  }
}

/**
 * 使用 Jupiter API 获取交易信息
 * @param {string} signature - 交易签名
 * @returns {Promise<Object|null>} Jupiter 交易信息
 */
async function getJupiterTradeInfo(signature) {
  try {
    const response = await fetch(
      `https://api.jup.ag/transactions/${signature}`,
      {
        method: 'GET',
        headers: {
          'Accept': 'application/json'
        }
      }
    );
    
    if (!response.ok) {
      if (response.status === 404) {
        // Jupiter API 可能没有这个交易的信息
        return null;
      }
      throw new Error(`Jupiter API 返回错误: ${response.status}`);
    }
    
    const data = await response.json();
    
    return {
      inputMint: data.inputMint,
      outputMint: data.outputMint,
      inputAmount: data.inputAmount,
      outputAmount: data.outputAmount,
      price: data.price,
      fee: data.fee,
      route: data.routePlan,
      dex: 'jupiter'
    };
  } catch (error) {
    console.error('获取 Jupiter 交易信息失败:', error.message);
    return null;
  }
}

/**
 * 解析 Raydium 交易
 * @param {Object} parsedTx - 解析后的交易对象
 * @returns {Object|null} Raydium 交易信息
 */
function parseRaydiumTrade(parsedTx) {
  if (!parsedTx || !parsedTx.transaction || !parsedTx.meta) {
    return null;
  }
  
  const { meta, transaction: txData } = parsedTx;
  const instructions = txData.message.instructions || [];
  
  // Raydium 程序 ID
  const RAYDIUM_PROGRAM_ID = '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8';
  
  // 查找 Raydium 程序指令
  const raydiumInstructions = instructions.filter(ix => {
    const programId = ix.programId?.toString() || ix.programId;
    return programId === RAYDIUM_PROGRAM_ID;
  });
  
  if (raydiumInstructions.length === 0) {
    return null;
  }
  
  // 解析代币余额变化
  const tokenChanges = parseTokenBalanceChanges(meta);
  
  // 查找卖出的代币（减少）和买入的代币（增加）
  const soldToken = tokenChanges.find(change => change.change < 0);
  const boughtToken = tokenChanges.find(change => change.change > 0);
  
  if (!soldToken || !boughtToken) {
    return null;
  }
  
  return {
    type: 'swap',
    soldToken: soldToken.mint,
    soldAmount: Math.abs(soldToken.change),
    boughtToken: boughtToken.mint,
    boughtAmount: boughtToken.change,
    dex: 'raydium',
    tokenChanges: tokenChanges
  };
}

/**
 * 解析代币余额变化
 * @param {Object} meta - 交易元数据
 * @returns {Array} 代币余额变化列表
 */
function parseTokenBalanceChanges(meta) {
  const changes = [];
  const preBalances = meta.preTokenBalances || [];
  const postBalances = meta.postTokenBalances || [];
  
  // 创建映射以便匹配
  const balanceMap = new Map();
  
  // 处理交易前的代币余额
  preBalances.forEach(preBalance => {
    const key = `${preBalance.accountIndex}-${preBalance.mint}`;
    balanceMap.set(key, {
      mint: preBalance.mint,
      owner: preBalance.owner,
      accountIndex: preBalance.accountIndex,
      preAmount: parseFloat(preBalance.uiTokenAmount?.uiAmountString || '0'),
      postAmount: 0
    });
  });
  
  // 处理交易后的代币余额
  postBalances.forEach(postBalance => {
    const key = `${postBalance.accountIndex}-${postBalance.mint}`;
    const existing = balanceMap.get(key);
    if (existing) {
      existing.postAmount = parseFloat(postBalance.uiTokenAmount?.uiAmountString || '0');
    } else {
      // 新增的代币账户
      balanceMap.set(key, {
        mint: postBalance.mint,
        owner: postBalance.owner,
        accountIndex: postBalance.accountIndex,
        preAmount: 0,
        postAmount: parseFloat(postBalance.uiTokenAmount?.uiAmountString || '0')
      });
    }
  });
  
  // 计算变化
  balanceMap.forEach((balance, key) => {
    const change = balance.postAmount - balance.preAmount;
    if (Math.abs(change) > 0.00000001) { // 忽略微小变化
      changes.push({
        ...balance,
        change: change
      });
    }
  });
  
  return changes;
}

/**
 * 解析交易的买入/卖出信息（整合多种方法）
 * @param {string} signature - 交易签名
 * @returns {Promise<Object>} 买入/卖出信息
 */
export async function parseTransactionBuySellInfo(signature) {
  try {
    // 检查是否有可用的 RPC 端点
    if (!config.drpc.apiKey && !config.helius.apiKey) {
      throw new Error('需要配置 dRPC 或 Helius API Key 才能使用此功能');
    }
    
    // 优先使用 dRPC，如果没有则使用配置的 RPC 端点
    const rpcEndpoint = config.drpc.apiKey
      ? `https://lb.drpc.live/solana/${config.drpc.apiKey}`
      : config.solana.rpcEndpoint;
    
    const connection = new Connection(rpcEndpoint, 'confirmed');
    
    // 1. 使用 getParsedTransactionsBatch 获取交易详情
    console.log(`正在获取交易 ${signature} 的详情...`);
    const parsedTransactions = await getParsedTransactionsBatch(
      connection,
      [signature],
      1
    );
    
    if (!parsedTransactions || parsedTransactions.length === 0 || !parsedTransactions[0]) {
      throw new Error('无法获取交易详情，交易可能不存在或已过期');
    }
    
    const parsedTx = parsedTransactions[0];
    
    // 2. 提取交易信息
    const accountKeys = parsedTx?.transaction?.message?.accountKeys || [];
    const meta = parsedTx?.meta || {};
    const preBalances = meta.preBalances || [];
    const postBalances = meta.postBalances || [];
    const preTokenBalances = meta.preTokenBalances || [];
    const postTokenBalances = meta.postTokenBalances || [];
    
    // 3. 使用现有的 determineTransactionType 方法解析
    const transactionType = determineTransactionType(
      parsedTx,
      accountKeys,
      preBalances,
      postBalances,
      preTokenBalances,
      postTokenBalances
    );
    
    // 4. 尝试使用 Jupiter API 获取信息（优先级最高）
    let jupiterInfo = null;
    try {
      jupiterInfo = await getJupiterTradeInfo(signature);
    } catch (error) {
      console.warn('Jupiter API 查询失败:', error.message);
    }
    
    // 5. 尝试解析 Raydium 交易
    let raydiumInfo = null;
    try {
      raydiumInfo = parseRaydiumTrade(parsedTx);
    } catch (error) {
      console.warn('Raydium 解析失败:', error.message);
    }
    
    // 6. 整合所有信息
    const result = {
      signature: signature,
      transactionType: transactionType.type, // 'buy', 'sell', 'transfer', 'unknown'
      description: transactionType.description,
      
      // 从 determineTransactionType 获取的信息
      fromBalanceAnalysis: {
        type: transactionType.type,
        solAmount: transactionType.details?.solAmount || null,
        tokenAmount: transactionType.details?.tokenAmount || null,
        tokenMint: transactionType.details?.tokenMint || null,
        price: transactionType.details?.price || null,
        direction: transactionType.details?.direction || null
      },
      
      // Jupiter API 信息（如果有）
      fromJupiter: jupiterInfo ? {
        inputMint: jupiterInfo.inputMint,
        outputMint: jupiterInfo.outputMint,
        inputAmount: jupiterInfo.inputAmount,
        outputAmount: jupiterInfo.outputAmount,
        price: jupiterInfo.price,
        fee: jupiterInfo.fee,
        route: jupiterInfo.route
      } : null,
      
      // Raydium 解析信息（如果有）
      fromRaydium: raydiumInfo ? {
        type: raydiumInfo.type,
        soldToken: raydiumInfo.soldToken,
        soldAmount: raydiumInfo.soldAmount,
        boughtToken: raydiumInfo.boughtToken,
        boughtAmount: raydiumInfo.boughtAmount
      } : null,
      
      // 最终整合的买入/卖出信息（优先级：Jupiter > Raydium > 余额分析）
      buySellInfo: null
    };
    
    // 7. 整合最终结果（优先级：Jupiter > Raydium > 余额分析）
    if (jupiterInfo) {
      // 使用 Jupiter 信息
      const WSOL_MINT = 'So11111111111111111111111111111111111111112';
      const isBuy = jupiterInfo.inputMint === WSOL_MINT;
      
      result.buySellInfo = {
        type: isBuy ? 'buy' : 'sell',
        source: 'jupiter',
        solAmount: isBuy ? (jupiterInfo.inputAmount / 1e9) : (jupiterInfo.outputAmount / 1e9),
        tokenAmount: isBuy ? (jupiterInfo.outputAmount / 1e9) : (jupiterInfo.inputAmount / 1e9),
        tokenMint: isBuy ? jupiterInfo.outputMint : jupiterInfo.inputMint,
        price: jupiterInfo.price,
        fee: jupiterInfo.fee ? (jupiterInfo.fee / 1e9) : null
      };
    } else if (raydiumInfo) {
      // 使用 Raydium 信息
      const WSOL_MINT = 'So11111111111111111111111111111111111111112';
      const isBuy = raydiumInfo.soldToken === WSOL_MINT;
      
      result.buySellInfo = {
        type: isBuy ? 'buy' : 'sell',
        source: 'raydium',
        solAmount: isBuy ? raydiumInfo.soldAmount : raydiumInfo.boughtAmount,
        tokenAmount: isBuy ? raydiumInfo.boughtAmount : raydiumInfo.soldAmount,
        tokenMint: isBuy ? raydiumInfo.boughtToken : raydiumInfo.soldToken,
        price: null // Raydium 解析中不包含价格
      };
    } else if (transactionType.details && transactionType.details.solAmount) {
      // 使用余额分析信息
      result.buySellInfo = {
        type: transactionType.type,
        source: 'balance_analysis',
        solAmount: transactionType.details.solAmount,
        tokenAmount: transactionType.details.tokenAmount || null,
        tokenMint: transactionType.details.tokenMint || null,
        price: transactionType.details.price || null
      };
    }
    
    return result;
  } catch (error) {
    console.error('解析交易买入/卖出信息失败:', error);
    throw error;
  }
}

/**
 * 带重试的 RPC 调用
 */
async function rpcCallWithRetry(connection, method, args, maxRetries = 3, retryDelay = 1000) {
  for (let attempt = 0; attempt < maxRetries; attempt++) {
    try {
      if (attempt > 0) {
        // 指数退避：第1次重试等1秒，第2次等2秒，第3次等4秒
        const waitTime = retryDelay * Math.pow(2, attempt - 1);
        console.log(`RPC 调用失败，${waitTime}ms 后重试 (${attempt + 1}/${maxRetries})...`);
        await delay(waitTime);
      }
      
      const result = await method.apply(connection, args);
      return result;
    } catch (error) {
      const isRateLimit = error.message && (
        error.message.includes('429') || 
        error.message.includes('Too Many Requests') ||
        error.message.includes('-32429')
      );
      
      if (isRateLimit && attempt < maxRetries - 1) {
        // 如果是速率限制，等待更长时间
        const waitTime = retryDelay * Math.pow(2, attempt) * 2; // 速率限制时等待更久
        console.log(`遇到速率限制，${waitTime}ms 后重试 (${attempt + 1}/${maxRetries})...`);
        await delay(waitTime);
        continue;
      }
      
      if (attempt === maxRetries - 1) {
        throw error;
      }
    }
  }
}

/**
 * 批量获取交易详情（分批处理，避免一次性请求太多）
 * 针对免费版 Helius 的严格速率限制进行优化
 */
async function getParsedTransactionsBatch(connection, signatureList, batchSize = 10) {
  const allTransactions = [];
  
  // 分批处理（免费版 Helius 建议使用更小的批次）
  for (let i = 0; i < signatureList.length; i += batchSize) {
    const batch = signatureList.slice(i, i + batchSize);
    const batchNumber = Math.floor(i / batchSize) + 1;
    const totalBatches = Math.ceil(signatureList.length / batchSize);
    
    try {
      console.log(`正在获取批次 ${batchNumber}/${totalBatches} (${batch.length} 个交易)...`);
      
      // 使用重试机制获取批次（增加重试次数和延迟）
      const batchTransactions = await rpcCallWithRetry(
        connection,
        connection.getParsedTransactions.bind(connection),
        [batch, { maxSupportedTransactionVersion: 0 }],
        5, // 增加重试次数到 5 次
        2000 // 增加初始延迟到 2 秒
      );
      
      allTransactions.push(...(batchTransactions || []));
      console.log(`批次 ${batchNumber} 获取成功`);
      
      // 批次之间添加更长的延迟，避免速率限制（免费版需要更保守）
      if (i + batchSize < signatureList.length) {
        const delayTime = 1000; // 每个批次之间等待 1 秒（免费版建议）
        console.log(`等待 ${delayTime}ms 后继续下一批次...`);
        await delay(delayTime);
      }
    } catch (error) {
      const isRateLimit = error.message && (
        error.message.includes('429') || 
        error.message.includes('Too Many Requests') ||
        error.message.includes('-32429')
      );
      
      if (isRateLimit) {
        console.error(`批次 ${batchNumber} 遇到速率限制，等待更长时间后重试...`);
        // 速率限制时等待更长时间
        await delay(5000); // 等待 5 秒
        
        // 尝试再次获取这个批次
        try {
          const batchTransactions = await rpcCallWithRetry(
            connection,
            connection.getParsedTransactions.bind(connection),
            [batch, { maxSupportedTransactionVersion: 0 }],
            3,
            3000
          );
          allTransactions.push(...(batchTransactions || []));
          console.log(`批次 ${batchNumber} 重试成功`);
        } catch (retryError) {
          console.error(`批次 ${batchNumber} 重试失败:`, retryError.message);
          // 如果重试也失败，填充 null
          allTransactions.push(...new Array(batch.length).fill(null));
        }
      } else {
        console.error(`获取批次 ${batchNumber} 失败:`, error.message);
        // 如果某个批次失败，填充 null
        allTransactions.push(...new Array(batch.length).fill(null));
      }
    }
  }
  
  return allTransactions;
}

/**
 * 获取地址的详细交易历史（优先使用 dRPC，备选 Helius RPC，类似 QuickNode 文档中的实现）
 * 参考: https://learnblockchain.cn/article/11171
 * @param {string} address - 要查询的地址（钱包、程序ID、代币铸币地址等）
 * @param {number} limit - 返回的最大交易数量（默认100，最大1000）
 * @returns {Promise<Array>} 交易历史数组，包含签名、时间、状态、程序交互等信息
 */
export async function getAddressTransactionHistory(address, limit = 100) {
  // 检查是否有可用的 RPC 端点（优先 dRPC，备选 Helius）
  if (!config.drpc.apiKey && !config.helius.apiKey) {
    throw new Error('需要配置 dRPC 或 Helius API Key 才能使用此功能');
  }

  // 优先使用 dRPC，如果没有则使用配置的 RPC 端点（可能是 Helius）
  const rpcEndpoint = config.drpc.apiKey
    ? `https://lb.drpc.live/solana/${config.drpc.apiKey}`
    : config.solana.rpcEndpoint;

  const rpcProvider = config.drpc.apiKey ? 'dRPC' : (config.helius.apiKey ? 'Helius' : 'Public RPC');
  console.log(`使用 RPC 端点: ${rpcProvider} (${rpcEndpoint.substring(0, 50)}...)`);

  const connection = new Connection(rpcEndpoint, 'confirmed');
  const publicKey = new PublicKey(address);
  
  try {
    // 1. 获取交易签名列表（使用重试机制）
    console.log(`正在获取地址 ${address} 的交易签名列表（限制: ${limit}）...`);
    const transactionList = await rpcCallWithRetry(
      connection,
      connection.getSignaturesForAddress.bind(connection),
      [publicKey, { limit: Math.min(limit, 1000) }],
      3,
      1000
    );

    if (!transactionList || transactionList.length === 0) {
      return [];
    }

    console.log(`获取到 ${transactionList.length} 个交易签名，开始获取交易详情...`);

    // 2. 提取签名列表
    const signatureList = transactionList.map(tx => tx.signature);

    // 3. 批量获取解析后的交易详情（分批处理，避免速率限制）
    // 免费版 Helius 建议使用更小的批次（10个）和更长的延迟
    const transactionDetails = await getParsedTransactionsBatch(
      connection,
      signatureList,
      10 // 每批处理 10 个交易（免费版建议更小的批次）
    );

    // 4. 组合数据，返回详细的交易信息
    const detailedTransactions = transactionList.map((transaction, i) => {
      const date = transaction.blockTime ? new Date(transaction.blockTime * 1000) : null;
      const parsedTx = transactionDetails[i];
      
      // 如果交易详情获取失败，返回基本信息
      if (!parsedTx) {
        return {
          signature: transaction.signature,
          date: date ? date.toISOString() : null,
          dateFormatted: date ? date.toLocaleString('zh-CN') : null,
          transactionType: 'unknown',
          amount: null,
          solAmount: null,
          tokenAmount: null
        };
      }
      
      // 提取程序交互信息
      const instructions = parsedTx?.transaction?.message?.instructions || [];
      const programInteractions = instructions.map((instruction, n) => ({
        index: n + 1,
        programId: instruction.programId?.toString() || 'Unknown',
        programName: instruction.programId?.toString() || 'Unknown',
        // 如果是解析后的指令，可能包含更多信息
        parsed: instruction.parsed || null
      }));

      // 提取账户信息
      const accountKeys = parsedTx?.transaction?.message?.accountKeys || [];
      const accounts = accountKeys.map(account => ({
        pubkey: account.pubkey?.toString() || account.toString(),
        signer: account.signer || false,
        writable: account.writable || false
      }));

      // 提取元数据信息
      const meta = parsedTx?.meta || {};
      const fee = meta.fee || 0;
      const preBalances = meta.preBalances || [];
      const postBalances = meta.postBalances || [];
      const preTokenBalances = meta.preTokenBalances || [];
      const postTokenBalances = meta.postTokenBalances || [];
      const logMessages = meta.logMessages || [];
      const err = meta.err || transaction.err || null;

      // 判断交易类型（买入/卖出）
      const transactionType = determineTransactionType(
        parsedTx,
        accountKeys,
        preBalances,
        postBalances,
        preTokenBalances,
        postTokenBalances
      );

      // 提取金额信息
      let amount = null;
      let solAmount = null;
      let tokenAmount = null;
      
      // 优先从 details 中获取金额
      if (transactionType.details && transactionType.details.solAmount) {
        solAmount = transactionType.details.solAmount;
        tokenAmount = transactionType.details.tokenAmount || null;
        amount = solAmount;
      } else {
        // 如果 details 中没有金额，从余额变化中计算
        // 计算 SOL 变化总量（绝对值）
        if (preBalances && postBalances) {
          let totalSolChange = 0;
          for (let i = 0; i < Math.min(preBalances.length, postBalances.length); i++) {
            const solChange = (postBalances[i] - preBalances[i]) / 1e9; // 转换为 SOL
            totalSolChange += Math.abs(solChange);
          }
          if (totalSolChange > 0.0001) { // 忽略微小变化
            solAmount = totalSolChange;
            amount = solAmount;
          }
        }
        
        // 计算代币变化总量（绝对值）
        if (preTokenBalances && postTokenBalances) {
          const tokenBalanceMap = new Map();
          
          // 处理交易前的代币余额
          preTokenBalances.forEach(balance => {
            const key = `${balance.accountIndex}-${balance.mint}`;
            tokenBalanceMap.set(key, {
              preAmount: parseFloat(balance.uiTokenAmount?.uiAmountString || '0'),
              postAmount: 0
            });
          });
          
          // 处理交易后的代币余额
          postTokenBalances.forEach(balance => {
            const key = `${balance.accountIndex}-${balance.mint}`;
            const existing = tokenBalanceMap.get(key);
            if (existing) {
              existing.postAmount = parseFloat(balance.uiTokenAmount?.uiAmountString || '0');
            } else {
              tokenBalanceMap.set(key, {
                preAmount: 0,
                postAmount: parseFloat(balance.uiTokenAmount?.uiAmountString || '0')
              });
            }
          });
          
          // 计算总代币变化（绝对值）
          let totalTokenChange = 0;
          tokenBalanceMap.forEach((balance, key) => {
            const change = Math.abs(balance.postAmount - balance.preAmount);
            if (change > 0.00000001) { // 忽略微小变化
              totalTokenChange += change;
            }
          });
          
          if (totalTokenChange > 0) {
            tokenAmount = totalTokenChange;
          }
        }
      }

      // 只返回必要字段：买入/卖出、金额、签名、时间
      return {
        signature: transaction.signature,
        date: date ? date.toISOString() : null,
        dateFormatted: date ? date.toLocaleString('zh-CN') : null,
        transactionType: transactionType.type, // 'buy', 'sell', 'transfer', 'unknown'
        amount: amount, // SOL 金额
        solAmount: solAmount, // SOL 金额
        tokenAmount: tokenAmount // 代币数量
      };
    });

    console.log(`成功获取 ${detailedTransactions.length} 笔交易的详细信息`);
    return detailedTransactions;
  } catch (error) {
    console.error('获取地址交易历史失败:', error);
    
    // 提供更友好的错误信息
    if (error.message && (error.message.includes('429') || error.message.includes('Too Many Requests'))) {
      throw new Error('请求过于频繁，已达到 Helius API 速率限制。请稍后再试，或减少查询数量（limit 参数）。');
    }
    
    throw new Error(`获取地址 ${address} 的交易历史失败: ${error.message}`);
  }
}

/**
 * 构建 Helius API 请求配置
 */
function buildHeliusRequestConfig(address, options) {
  const {
    limit = 50,
    transactionDetails = 'signatures',
    sortOrder = 'desc',
    filters = {},
    paginationToken = null
  } = options;

  // Helius API 限制：当 transactionDetails 为 'full' 时，最多只能请求 100 笔交易
  const effectiveLimit = transactionDetails === 'full' ? Math.min(limit, 100) : Math.min(limit, 1000);

  const configObj = {
    transactionDetails,
    limit: effectiveLimit,
    sortOrder
  };

  // 添加过滤条件
  if (Object.keys(filters).length > 0) {
    configObj.filters = filters;
    
    // 警告：如果使用 blockTime 过滤，提醒用户可能不完整
    if (filters.blockTime) {
      console.warn('⚠️ 注意：blockTime 是"Estimated production time"（估计的生产时间），可能不准确，且某些交易的 blockTime 可能为 null');
      console.warn('   使用 blockTime 过滤可能导致数据不完整，建议优先使用 slot 过滤以获得更准确的结果');
    }
  }

  // 添加分页令牌
  if (paginationToken) {
    configObj.paginationToken = paginationToken;
  }

  const requestBody = {
    jsonrpc: '2.0',
    id: '1',
    method: 'getTransactionsForAddress',
    params: [address, configObj]
  };

  return { requestBody, configObj, effectiveLimit };
}

/**
 * 发送 Helius API 请求并处理响应
 */
async function sendHeliusGetTransactionsRequest(heliusRpcUrl, address, requestBody, configObj) {
  console.log(`使用 Helius getTransactionsForAddress 查询地址 ${address}...`);
  console.log(`请求参数:`, JSON.stringify({
    method: requestBody.method,
    address: address.substring(0, 20) + '...',
    limit: configObj.limit,
    transactionDetails: configObj.transactionDetails,
    sortOrder: configObj.sortOrder,
    hasFilters: Object.keys(configObj.filters || {}).length > 0,
    hasPaginationToken: !!configObj.paginationToken
  }, null, 2));

  const response = await fetch(heliusRpcUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(requestBody),
  });

  if (!response.ok) {
    const errorText = await response.text();
    throw new Error(`Helius API 返回错误: ${response.status} - ${errorText}`);
  }

  const result = await response.json();

  if (result.error) {
    throw new Error(`Helius API 错误: ${result.error.message || JSON.stringify(result.error)}`);
  }

  return result.result;
}

/**
 * 记录交易排序信息
 */
function logTransactionSortInfo(transactions, sortOrder) {
  if (transactions.length === 0) return;

  const firstTx = transactions[0];
  const lastTx = transactions[transactions.length - 1];
  const firstTime = firstTx.blockTime ? new Date(firstTx.blockTime * 1000).toISOString() : 'N/A';
  const lastTime = lastTx.blockTime ? new Date(lastTx.blockTime * 1000).toISOString() : 'N/A';
  
  console.log(`  排序方式: ${sortOrder === 'desc' ? '降序（从新到旧）' : '升序（从旧到新）'}`);
  console.log(`  第一条交易: blockTime=${firstTime}, 最后一条交易: blockTime=${lastTime}`);
}

/**
 * 过滤失败的交易
 */
function filterFailedTransactions(transactions, filters) {
  let needsClientSideFilter = false;
  
  if (filters.status === 'succeeded' || (!filters.status)) {
    // 检查 Helius 是否已经过滤了失败的交易
    if (filters.status === 'succeeded') {
      const hasFailedTx = transactions.some(tx => tx.err || tx.meta?.err);
      if (hasFailedTx) {
        needsClientSideFilter = true;
        console.log(`⚠ Helius API 可能不支持 filters.status，使用客户端过滤`);
      } else {
        console.log(`✓ Helius API 已通过 filters.status 过滤失败的交易`);
      }
    } else {
      // 默认情况下，过滤掉失败的交易
      needsClientSideFilter = true;
    }
    
    if (needsClientSideFilter) {
      const beforeFailedFilter = transactions.length;
      transactions = transactions.filter(tx => {
        const hasError = tx.err || tx.meta?.err;
        return !hasError;
      });
      const failedFilteredCount = beforeFailedFilter - transactions.length;
      if (failedFilteredCount > 0) {
        console.log(`✓ 客户端过滤掉 ${failedFilteredCount} 笔失败的交易`);
      }
    }
  } else if (filters.status === 'failed') {
    // 检查 Helius 是否已经过滤了成功的交易
    const hasSuccessTx = transactions.some(tx => !tx.err && !tx.meta?.err);
    if (hasSuccessTx) {
      needsClientSideFilter = true;
      console.log(`⚠ Helius API 可能不支持 filters.status，使用客户端过滤`);
    } else {
      console.log(`✓ Helius API 已通过 filters.status 过滤成功的交易`);
    }
    
    if (needsClientSideFilter) {
      const beforeSuccessFilter = transactions.length;
      transactions = transactions.filter(tx => {
        const hasError = tx.err || tx.meta?.err;
        return hasError;
      });
      const successFilteredCount = beforeSuccessFilter - transactions.length;
      if (successFilteredCount > 0) {
        console.log(`✓ 客户端过滤掉 ${successFilteredCount} 笔成功的交易（只显示失败的交易）`);
      }
    }
  }

  return transactions;
}

/**
 * 从交易中获取所有指令（包括 innerInstructions）
 */
function getAllInstructionsFromTransaction(tx) {
  const message = tx.transaction?.message;
  if (!message) return [];
  
  const accountKeys = message.accountKeys || [];
  const instructions = message.instructions || [];
  const innerInstructions = tx.meta?.innerInstructions || [];
  
  // 辅助函数：从指令中获取 programId
  const getProgramId = (ix) => {
    // 如果是 parsed 格式，直接使用 programId
    if (ix.programId) {
      return typeof ix.programId === 'string' ? ix.programId : ix.programId.toString();
    }
    
    // 如果是 unparsed 格式，从 accountKeys 中获取
    if (typeof ix.programIdIndex === 'number') {
      const account = accountKeys[ix.programIdIndex];
      if (account) {
        // account 可能是 PublicKey 对象或字符串
        return typeof account === 'string' ? account : (account.pubkey?.toString() || account.toString());
      }
    }
    
    return null;
  };
  
  // 处理主指令
  const allInstructions = instructions.map(ix => ({
    ...ix,
    _programId: getProgramId(ix) // 添加解析后的 programId
  }));
  
  // 处理 innerInstructions
  innerInstructions.forEach(inner => {
    if (Array.isArray(inner.instructions)) {
      inner.instructions.forEach(ix => {
        allInstructions.push({
          ...ix,
          _programId: getProgramId(ix)
        });
      });
    }
  });
  
  return allInstructions;
}

/**
 * 检查指令是否为系统辅助指令
 */
function isSystemInstruction(ix, tx) {
  const COMPUTE_BUDGET_PROGRAM = 'ComputeBudget111111111111111111111111111111';
  const SYSTEM_INSTRUCTION_NAMES = [
    'setcomputunitlimit',
    'setcomputunitprice',
    'setloadedaccountsdatasizelimit',
    'setloadedaccountsdatasizelimit' // 完整名称
  ];

  // 优先使用解析后的 programId
  let programId = ix._programId;
  
  // 如果没有解析后的 programId，尝试从指令本身获取
  if (!programId) {
    if (ix.programId) {
      programId = typeof ix.programId === 'string' ? ix.programId : ix.programId.toString();
    } else if (typeof ix.programIdIndex === 'number' && tx.transaction?.message?.accountKeys) {
      const account = tx.transaction.message.accountKeys[ix.programIdIndex];
      if (account) {
        programId = typeof account === 'string' ? account : (account.pubkey?.toString() || account.toString());
      }
    }
  }
  
  if (!programId) return false;
  
  programId = programId.toLowerCase();
  
  // 检查程序 ID 是否为 ComputeBudgetProgram
  if (programId === COMPUTE_BUDGET_PROGRAM.toLowerCase()) {
    return true;
  }
  
  // 如果是 parsed 格式的指令，检查指令名称
  if (ix.parsed && typeof ix.parsed === 'object') {
    const instructionName = (ix.parsed.type || '').toLowerCase();
    if (SYSTEM_INSTRUCTION_NAMES.some(name => 
      instructionName.includes(name) || name.includes(instructionName)
    )) {
      return true;
    }
  }
  
  return false;
}

/**
 * 检查交易是否有代币余额变化
 */
function hasTokenBalanceChanges(tx) {
  const preTokenBalances = tx.meta?.preTokenBalances || [];
  const postTokenBalances = tx.meta?.postTokenBalances || [];
  
  if (preTokenBalances.length === 0 && postTokenBalances.length === 0) {
    return false;
  }
  
  // 检查是否有实际的数量变化
  const tokenBalanceMap = new Map();
  
  // 收集 post token balances
  postTokenBalances.forEach(post => {
    const key = `${post.owner || post.account}-${post.mint}`;
    tokenBalanceMap.set(key, {
      mint: post.mint,
      owner: post.owner || post.account,
      postAmount: post.uiTokenAmount?.uiAmount || post.amount || 0
    });
  });
  
  // 匹配 pre token balances 并检查变化
  let hasSignificantChange = false;
  preTokenBalances.forEach(pre => {
    const key = `${pre.owner || pre.account}-${pre.mint}`;
    const post = tokenBalanceMap.get(key);
    if (post) {
      const preAmount = pre.uiTokenAmount?.uiAmount || pre.amount || 0;
      const change = Math.abs(post.postAmount - preAmount);
      // 如果有大于 0.000001 的变化，认为是有意义的代币变化
      if (change > 0.000001) {
        hasSignificantChange = true;
      }
    } else {
      // 如果 pre 存在但 post 不存在，说明代币被完全转出
      const preAmount = pre.uiTokenAmount?.uiAmount || pre.amount || 0;
      if (preAmount > 0.000001) {
        hasSignificantChange = true;
      }
    }
  });
  
  // 检查是否有新的代币（只有 post 没有 pre）
  if (!hasSignificantChange) {
    for (const [key, post] of tokenBalanceMap.entries()) {
      const hasPre = preTokenBalances.some(pre => 
        `${pre.owner || pre.account}-${pre.mint}` === key
      );
      if (!hasPre && post.postAmount > 0.000001) {
        hasSignificantChange = true;
        break;
      }
    }
  }
  
  return hasSignificantChange;
}

/**
 * 检查交易是否有 SOL 余额变化
 */
function hasSOLBalanceChanges(tx) {
  const preBalances = tx.meta?.preBalances || [];
  const postBalances = tx.meta?.postBalances || [];
  
  if (preBalances.length === 0 || postBalances.length === 0) {
    return false;
  }
  
  // 检查是否有大于 0.0001 SOL 的变化（排除手续费）
  for (let i = 0; i < Math.min(preBalances.length, postBalances.length); i++) {
    const change = Math.abs(postBalances[i] - preBalances[i]);
    // 大于 100000 lamports (0.0001 SOL) 的变化
    if (change > 100000) {
      return true;
    }
  }
  
  return false;
}

/**
 * 过滤系统辅助指令的交易
 */
function filterSystemInstructionTransactions(transactions, skipSystemInstructionFilter) {
  const hasFullTransactionData = transactions.length > 0 && transactions.some(tx => 
    tx.transaction && tx.meta
  );
  
  if (!hasFullTransactionData || skipSystemInstructionFilter) {
    return transactions;
  }

  const filteredTransactions = transactions.filter(tx => {
    // 如果没有 transaction 字段，保留（可能是 signatures 模式）
    if (!tx.transaction || !tx.transaction.message) {
      return true;
    }
    
    // 获取所有指令（包括 innerInstructions）
    const allInstructions = getAllInstructionsFromTransaction(tx);
    
    // 如果没有任何指令，过滤掉
    if (allInstructions.length === 0) {
      return false;
    }
    
    // 检查是否所有指令都是系统辅助指令
    const allSystemInstructions = allInstructions.every(ix => isSystemInstruction(ix, tx));
    
    // 如果所有指令都是系统辅助指令，过滤掉
    if (allSystemInstructions) {
      return false;
    }
    
    // 检查是否有实际的业务操作：代币余额变化或 SOL 余额变化
    const hasTokenChanges = hasTokenBalanceChanges(tx);
    const hasSOLChanges = hasSOLBalanceChanges(tx);
    
    // 如果没有实际的业务操作（没有代币变化也没有 SOL 变化），过滤掉
    if (!hasTokenChanges && !hasSOLChanges) {
      return false;
    }
    
    return true;
  });
  
  const filteredCount = transactions.length - filteredTransactions.length;
  if (filteredCount > 0) {
    console.log(`✓ 过滤掉 ${filteredCount} 笔只包含系统辅助指令的交易（SetComputeUnitLimit/SetComputeUnitPrice/setLoadedAccountsDataSizeLimit）`);
  }
  
  // 调试：如果还有交易没有被过滤，打印前几个的指令信息（用于排查）
  if (filteredTransactions.length > 0 && filteredCount < transactions.length) {
    const remainingCount = filteredTransactions.length;
    const sampleCount = Math.min(3, remainingCount);
    
    for (let i = 0; i < sampleCount; i++) {
      const tx = filteredTransactions[i];
      const allInstructions = getAllInstructionsFromTransaction(tx);
      const instructionInfo = allInstructions.slice(0, 5).map((ix, idx) => {
        const programId = ix._programId || ix.programId?.toString() || 'unknown';
        const parsedType = ix.parsed?.type || 'unparsed';
        const isSystem = isSystemInstruction(ix, tx);
        const programIdStr = typeof programId === 'string' ? programId : programId.toString();
        return `  [${idx}] programId: ${programIdStr.length > 20 ? programIdStr.substring(0, 20) + '...' : programIdStr}, type: ${parsedType}, isSystem: ${isSystem}`;
      }).join('\n');
      
      const signature = tx.signature || tx.transaction?.signatures?.[0] || 'N/A';
      const signatureStr = typeof signature === 'string' ? signature : signature.toString();
      console.log(`  交易 ${i + 1} (签名: ${signatureStr.length > 20 ? signatureStr.substring(0, 20) + '...' : signatureStr}):`);
      console.log(instructionInfo);
      if (allInstructions.length > 5) {
        console.log(`  ... 还有 ${allInstructions.length - 5} 个指令`);
      }
    }
  }
  
  return filteredTransactions;
}

/**
 * 格式化 full 模式的交易数据
 */
function formatFullTransactionData(transactions) {
  if (transactions.length === 0) return transactions;
  
  const sampleTx = transactions[0];
  if (!sampleTx.transaction || !sampleTx.meta) {
    return transactions;
  }

  console.log(`✓ 使用 transactionDetails: 'full' 返回的完整交易数据（无需额外 RPC 调用）`);
  
  // 将数据格式化为标准的 parsed transaction 格式，方便后续使用
  return transactions.map(tx => {
    // 确保 signature 字段存在：优先使用 tx.signature，如果没有则从 transaction.signatures[0] 获取
    const signature = tx.signature || tx.transaction?.signatures?.[0] || null;

    // todo 有签名放这里去打印给cursor调试解析
    // const DEBUG_SIGNATURES = [
    //   '有签名放这里去打印给cursor调试解析'
    // ];
    // if (signature && DEBUG_SIGNATURES.includes(signature)) {
    //   console.log(`\n[formatFullTransactionData] 调试签名 ${signature} 的完整交易详情:`);
    //   console.log(JSON.stringify(tx, null, 2));
    // }
    
    // 如果已经有 transaction 和 meta，格式化为标准格式
    if (tx.transaction && tx.meta) {
      return {
        ...tx,
        signature: signature || tx.signature, // 确保 signature 字段存在
        // 保持原始数据
        parsedTransaction: {
          transaction: tx.transaction,
          meta: tx.meta,
          slot: tx.slot,
          blockTime: tx.blockTime,
          version: tx.version
        }
      };
    }
    return {
      ...tx,
      signature: signature || tx.signature // 确保 signature 字段存在
    };
  });
}

/**
 * 批量解析交易的完整详情
 */
async function parseFullTransactionsForAddress(transactions, filters) {
  if (transactions.length === 0) return transactions;

  console.log(`开始批量解析 ${transactions.length} 笔交易的完整详情...`);
  
  // 提取签名列表
  const signatures = transactions
    .map(tx => tx.signature)
    .filter(sig => sig);

  if (signatures.length === 0) return transactions;

  try {
    // 优先使用 dRPC（如果配置了），否则使用 Helius RPC
    const rpcEndpoint = config.drpc.apiKey
      ? `https://lb.drpc.live/solana/${config.drpc.apiKey}`
      : `https://mainnet.helius-rpc.com/?api-key=${config.helius.apiKey}`;

    const { Connection } = await import('@solana/web3.js');
    const connection = new Connection(rpcEndpoint, 'confirmed');

    // 批量解析交易（使用更大的批次，因为 Helius RPC 性能更好）
    const batchSize = signatures.length <= 50 ? signatures.length : 50;
    const parsedTransactions = await getParsedTransactionsBatch(
      connection,
      signatures,
      batchSize
    );

    // 将解析后的交易详情合并到原始交易数据中
    let parsedTransactionsList = transactions.map((tx, index) => {
      const parsedTx = parsedTransactions[index];
      if (parsedTx && parsedTx.transaction && parsedTx.meta) {
        // 将 parsedTransaction 的数据提取到顶层，方便过滤逻辑使用
        return {
          ...tx,
          transaction: parsedTx.transaction,
          meta: parsedTx.meta,
          parsedTransaction: parsedTx
        };
      }
      return {
        ...tx,
        parsedTransaction: parsedTx || null
      };
    });

    console.log(`✓ 成功解析 ${parsedTransactions.filter(tx => tx !== null).length} 笔交易的完整详情`);
    
    // 过滤失败的交易
    parsedTransactionsList = filterFailedTransactions(parsedTransactionsList, filters);
    
    // 过滤系统指令交易
    parsedTransactionsList = filterSystemInstructionTransactions(parsedTransactionsList, false);
    
    return parsedTransactionsList;
  } catch (parseError) {
    console.error('批量解析交易失败:', parseError.message);
    console.warn('继续返回签名级别的交易数据');
    return transactions;
  }
}

/**
 * 使用 Helius 增强 API getTransactionsForAddress 获取地址交易历史
 * 文档: https://www.helius.dev/docs/api-reference/rpc/http/gettransactionsforaddress
 * 支持高级过滤、排序和分页功能
 * 
 * @param {string} address - 要查询的地址（钱包、程序ID、代币铸币地址等）
 * @param {number} options.limit - 返回的最大交易数量（默认 100，最大 1000）
 * @param {string} options.transactionDetails - 交易详情级别：'signatures'（仅签名）或 'full'（完整详情）

 * @param {string} options.filters.status - 交易状态：'succeeded'（成功）或 'failed'（失败）
 * @param {Object} options.filters.blockTime - 时间范围：{ gte: 1641038400, lte: 1641038460 } (Unix 时间戳，秒)
 *                                            注意：blockTime 是"Estimated production time"（估计的生产时间），
 *                                            @param {string} options.paginationToken - 分页令牌（用于获取下一页）
 */
export async function getTransactionsForAddress(address, options = {}) {
  if (!config.helius.apiKey) {
    throw new Error('需要配置 Helius API Key 才能使用 getTransactionsForAddress 方法');
  }

  const {
    sortOrder = 'desc',
    filters = {},
    parseFullTransactions = false,
    skipSystemInstructionFilter = false // 是否跳过系统辅助指令过滤（用于需要获取所有交易的场景）
  } = options;

  const heliusRpcUrl = `https://mainnet.helius-rpc.com/?api-key=${config.helius.apiKey}`;

  try {
    // 1. 构建请求配置
    const { requestBody, configObj } = buildHeliusRequestConfig(address, options);

    // 2. 发送请求并获取响应
    const result = await sendHeliusGetTransactionsRequest(heliusRpcUrl, address, requestBody, configObj);

    const transactionCount = result?.data?.length || 0;
    console.log(`✓ 成功获取 ${transactionCount} 笔交易`);
    
    // 3. 记录排序信息
    if (transactionCount > 0) {
      logTransactionSortInfo(result.data, sortOrder);
    }

    let transactions = result?.data || [];
    const paginationTokenResult = result?.paginationToken || null;

    // 4. 过滤失败的交易
    transactions = filterFailedTransactions(transactions, filters);

    // 5. 过滤系统辅助指令的交易
    transactions = filterSystemInstructionTransactions(transactions, skipSystemInstructionFilter);

    // 6. 格式化 full 模式的交易数据
    if (options.transactionDetails === 'full' && transactions.length > 0) {
      transactions = formatFullTransactionData(transactions);
    } else if (parseFullTransactions && transactions.length > 0) {
      // 7. 批量解析交易, 这个是传统的解析，还要调用sol rpc根据签名批量获取交易信息
      transactions = await parseFullTransactionsForAddress(transactions, filters);
    }

    return {
      data: transactions,
      paginationToken: paginationTokenResult,
      total: transactions.length
    };
  } catch (error) {
    console.error('使用 Helius getTransactionsForAddress 获取交易历史失败:', error);
    throw new Error(`获取地址 ${address} 的交易历史失败: ${error.message}`);
  }
}

/**
 * 根据时间范围获取对应的 slot 范围（估算）
 * 方法：先查询少量交易，从返回的数据中获取 slot 范围
 * 
 * @param {string} address - 要查询的地址
 * @param {number} blockTimeGte - 开始时间（Unix 时间戳，秒）
 * @param {number} blockTimeLt - 结束时间（Unix 时间戳，秒）
 * @returns {Promise<{slotGte: number|null, slotLt: number|null, method: string}>} slot 范围
 */
export async function getSlotRangeFromTimeRange(address, blockTimeGte, blockTimeLt) {
  if (!config.helius.apiKey) {
    throw new Error('需要配置 Helius API Key 才能使用此功能');
  }

  try {
    // 方法1：查询少量交易，从返回的数据中获取 slot 范围
    // 先查询开始时间附近的交易
    const startResult = await getTransactionsForAddress(address, {
      limit: 10,
      transactionDetails: 'signatures',
      sortOrder: 'desc',
      filters: {
        blockTime: {
          gte: blockTimeGte - 3600, // 提前1小时，确保能查到数据
          lte: blockTimeGte + 3600  // 延后1小时
        }
      }
    });

    // 再查询结束时间附近的交易
    const endResult = await getTransactionsForAddress(address, {
      limit: 10,
      transactionDetails: 'signatures',
      sortOrder: 'desc',
      filters: {
        blockTime: {
          gte: blockTimeLt - 3600, // 提前1小时
          lte: blockTimeLt + 3600  // 延后1小时
        }
      }
    });

    let slotGte = null;
    let slotLt = null;

    // 从开始时间的交易中找到最小的 slot（最接近 blockTimeGte）
    if (startResult.data && startResult.data.length > 0) {
      const relevantTxs = startResult.data.filter(tx => 
        tx.blockTime && tx.blockTime >= blockTimeGte
      );
      if (relevantTxs.length > 0) {
        slotGte = Math.min(...relevantTxs.map(tx => tx.slot));
      } else {
        // 如果没有精确匹配，使用最接近的
        slotGte = Math.min(...startResult.data.map(tx => tx.slot));
      }
    }

    // 从结束时间的交易中找到最大的 slot（最接近 blockTimeLt）
    if (endResult.data && endResult.data.length > 0) {
      const relevantTxs = endResult.data.filter(tx => 
        tx.blockTime && tx.blockTime <= blockTimeLt
      );
      if (relevantTxs.length > 0) {
        slotLt = Math.max(...relevantTxs.map(tx => tx.slot));
      } else {
        // 如果没有精确匹配，使用最接近的
        slotLt = Math.max(...endResult.data.map(tx => tx.slot));
      }
    }

    if (slotGte && slotLt) {
      // 添加一些缓冲，确保覆盖完整的时间范围
      // 每个 slot 约 400ms，1小时约 9000 个 slot
      const buffer = 10000; // 约1小时的缓冲
      return {
        slotGte: Math.max(0, slotGte - buffer),
        slotLt: slotLt + buffer,
        method: 'from_transactions'
      };
    }

    // 方法2：如果方法1失败，使用估算（基于当前 slot 和时间差）
    // 注意：这个方法不够精确，但可以作为备选
      const connection = new Connection(config.solana.rpcEndpoint, 'confirmed');
    const currentSlot = await connection.getSlot();
    const currentTime = Math.floor(Date.now() / 1000);
    
    // 估算：每个 slot 约 400ms，即每秒约 2.5 个 slot
    const slotsPerSecond = 2.5;
    const timeDiffStart = currentTime - blockTimeGte;
    const timeDiffEnd = currentTime - blockTimeLt;
    
    return {
      slotGte: Math.max(0, Math.floor(currentSlot - timeDiffStart * slotsPerSecond)),
      slotLt: Math.floor(currentSlot - timeDiffEnd * slotsPerSecond),
      method: 'estimated'
    };
  } catch (error) {
    console.error('获取 slot 范围失败:', error);
    throw error;
  }
}

/**
 * 获取代币持有人列表
 * 使用 Helius API 获取代币持有人数据
 * 注意：需要配置 Helius API Key 才能使用此功能
 * @param {string} tokenMintAddress - 代币地址
 * @param {number} offset - 偏移量
 * @param {number} limit - 限制数量
 * @param {boolean} enableClustering - 是否启用地址聚类（默认 false，因为较慢）
 * @returns {Promise<Array>} 持有人列表
 */
export async function getTokenHolders(tokenMintAddress, offset = 0, limit = 100) {

    try {
      console.log('尝试使用 Helius API 获取持有人...');
      const { getTokenHoldersViaHelius } = await import('../src/tokenTracker.js');
      const holders = await getTokenHoldersViaHelius(
        config.helius.apiKey,
        tokenMintAddress,
      limit + offset // 获取更多数据以应用 offset
    );
    
      // 应用 offset 和 limit
    const result = holders.slice(offset, offset + limit);
    console.log(`✅ Helius API 成功获取 ${result.length} 个持有人`);
    return result;
  } catch (heliusError) {
    console.error('Helius API 获取持有人失败:', heliusError.message);
    throw new Error(`获取持有人失败。Helius API 错误: ${heliusError.message}\n` +
      `建议：1) 检查 Helius API Key 是否正确 2) 验证代币地址是否正确: ${tokenMintAddress} 3) 检查网络连接`);
  }
}

