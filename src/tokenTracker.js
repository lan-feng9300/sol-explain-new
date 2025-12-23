import { Connection, PublicKey } from '@solana/web3.js';
import { config } from '../config/index.js';

/**
 * 获取代币的所有交易记录
 * @param {Connection} connection - Solana 连接对象
 * @param {string} tokenMintAddress - 代币的 mint 地址
 * @param {number} limit - 获取的交易数量限制
 * @returns {Promise<Array>} 交易记录数组
 */
export async function getTokenTransactions(connection, tokenMintAddress, limit = 100) {
  try {
    const mintPublicKey = new PublicKey(tokenMintAddress);
    
    // 方法1: 通过代币账户获取交易（适用于已知的代币账户）
    // 方法2: 通过程序日志搜索（更通用，但需要解析日志）
    // 方法3: 使用 DEX 程序账户（如 Raydium, Orca 等）
    
    // 这里我们使用一个混合方法：
    // 1. 获取所有与该代币 mint 地址相关的交易签名
    // 2. 解析每笔交易，提取买入/卖出信息
    
    const transactions = [];
    
    // 获取代币账户的交易签名
    // 注意：Solana 没有直接的方法获取某个代币的所有交易
    // 需要通过以下方式：
    // - 监听代币账户的变化
    // - 使用索引服务（如 Helius, QuickNode）
    // - 解析 DEX 程序账户
    
    // 这里我们演示如何通过已知的代币账户获取交易
    // 实际应用中，你需要：
    // 1. 维护一个代币账户列表
    // 2. 或者使用索引服务 API
    // 3. 或者监听链上事件
    
    return transactions;
  } catch (error) {
    console.error('获取代币交易失败:', error);
    throw error;
  }
}

/**
 * 解析交易，提取买入/卖出信息
 * @param {Object} transaction - Solana 交易对象
 * @param {PublicKey} tokenMint - 代币 mint 地址
 * @returns {Object|null} 解析后的交易信息
 */
export function parseTokenTransaction(transaction, tokenMint) {
  if (!transaction || !transaction.transaction) {
    return null;
  }
  
  try {
    const tx = transaction.transaction;
    const meta = transaction.meta;
    
    if (!meta || meta.err) {
      return null; // 交易失败，跳过
    }
    
    // 解析交易中的代币转移
    const preBalances = meta.preTokenBalances || [];
    const postBalances = meta.postTokenBalances || [];
    
    // 查找涉及目标代币的余额变化
    const tokenChanges = [];
    
    for (let i = 0; i < postBalances.length; i++) {
      const post = postBalances[i];
      if (post.mint === tokenMint.toString()) {
        const pre = preBalances.find(
          b => b.accountIndex === post.accountIndex && b.mint === post.mint
        );
        
        const change = {
          accountIndex: post.accountIndex,
          owner: post.owner,
          preAmount: pre ? parseFloat(pre.uiTokenAmount.uiAmountString || '0') : 0,
          postAmount: parseFloat(post.uiTokenAmount.uiAmountString || '0'),
          change: 0
        };
        
        change.change = change.postAmount - change.preAmount;
        
        if (change.change !== 0) {
          tokenChanges.push(change);
        }
      }
    }
    
    // 解析 SOL 转移（用于计算价格）
    const solChanges = [];
    const accountKeys = tx.message.accountKeys;
    
    if (meta.preBalances && meta.postBalances) {
      for (let i = 0; i < meta.postBalances.length; i++) {
        const preSol = meta.preBalances[i] / 1e9; // 转换为 SOL
        const postSol = meta.postBalances[i] / 1e9;
        const solChange = postSol - preSol;
        
        if (Math.abs(solChange) > 0.0001) { // 忽略微小变化
          solChanges.push({
            accountIndex: i,
            address: accountKeys[i].toString(),
            change: solChange
          });
        }
      }
    }
    
    // 判断是买入还是卖出
    // 买入：SOL 减少，代币增加
    // 卖出：SOL 增加，代币减少
    
    const result = {
      signature: transaction.transaction.signatures[0],
      blockTime: transaction.blockTime,
      slot: transaction.slot,
      tokenChanges: tokenChanges,
      solChanges: solChanges,
      type: null, // 'buy' 或 'sell'
      price: null, // 价格（SOL per token）
      amount: null, // 代币数量
      solAmount: null, // SOL 数量
      accounts: accountKeys.map(key => key.toString())
    };
    
    // 计算价格和类型
    if (tokenChanges.length > 0 && solChanges.length > 0) {
      const totalTokenChange = tokenChanges.reduce((sum, change) => sum + change.change, 0);
      const totalSolChange = solChanges.reduce((sum, change) => sum + Math.abs(change.change), 0);
      
      if (totalTokenChange > 0 && totalSolChange > 0) {
        // 买入：代币增加
        result.type = 'buy';
        result.amount = totalTokenChange;
        result.solAmount = totalSolChange;
        result.price = totalSolChange / totalTokenChange;
      } else if (totalTokenChange < 0 && totalSolChange > 0) {
        // 卖出：代币减少
        result.type = 'sell';
        result.amount = Math.abs(totalTokenChange);
        result.solAmount = totalSolChange;
        result.price = totalSolChange / Math.abs(totalTokenChange);
      }
    }
    
    return result;
  } catch (error) {
    console.error('解析交易失败:', error);
    return null;
  }
}

/**
 * 使用 Helius API 获取代币交易记录（推荐方法）
 * Helius 提供了强大的索引服务，可以轻松获取代币的所有交易
 * 注册地址: https://www.helius.dev/
 */
export async function getTokenTransactionsViaHelius(apiKey, tokenMintAddress, limit = 100) {
  try {
    const url = `https://api.helius.xyz/v0/addresses/${tokenMintAddress}/transactions?api-key=${apiKey}&limit=${limit}`;
    
    const response = await fetch(url);
    const data = await response.json();
    
    return data.map(tx => ({
      signature: tx.signature,
      timestamp: tx.timestamp,
      type: tx.type,
      source: tx.source,
      fee: tx.fee,
      feePayer: tx.feePayer,
      slot: tx.slot,
      nativeTransfers: tx.nativeTransfers || [],
      tokenTransfers: tx.tokenTransfers || [],
      accountData: tx.accountData || []
    }));
  } catch (error) {
    console.error('Helius API 请求失败:', error);
    throw error;
  }
}

/**
 * 使用 Solscan API 获取代币交易记录（免费，无需 API Key）
 * 注意：Solscan 的公开 API 可能有限制，建议使用 Helius 获得更好的体验
 */
export async function getTokenTransactionsViaSolscan(tokenMintAddress, limit = 100) {
  try {
    // 尝试多个 Solscan API 端点
    const endpoints = [
      `https://public-api.solscan.io/token/transactions?tokenAddress=${tokenMintAddress}&limit=${limit}`,
      `https://api.solscan.io/token/transactions?tokenAddress=${tokenMintAddress}&limit=${limit}`
    ];
    
    for (const url of endpoints) {
      try {
        // 创建超时控制器
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), 5000);
        
        const response = await fetch(url, {
          headers: {
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0'
          },
          signal: controller.signal
        });
        
        clearTimeout(timeoutId);
        
        if (!response.ok) {
          continue; // 尝试下一个端点
        }
        
        const data = await response.json();
        
        // 转换 Solscan 格式到 Helius 兼容格式
        if (Array.isArray(data)) {
          return data.map(tx => ({
            signature: tx.txHash || tx.signature,
            timestamp: tx.blockTime || Math.floor(Date.now() / 1000),
            type: tx.type || 'unknown',
            source: 'solscan',
            fee: tx.fee || 0,
            feePayer: tx.feePayer || '',
            slot: tx.slot || 0,
            nativeTransfers: tx.nativeTransfers || [],
            tokenTransfers: tx.tokenTransfers || (tx.tokenTransfers ? [{
              fromUserAccount: tx.from || '',
              toUserAccount: tx.to || '',
              tokenAmount: tx.amount || 0,
              mint: tokenMintAddress
            }] : []),
            accountData: []
          }));
        }
        
        // 如果返回的是对象，尝试提取数据
        if (data.data && Array.isArray(data.data)) {
          return data.data.map(tx => ({
            signature: tx.txHash || tx.signature,
            timestamp: tx.blockTime || Math.floor(Date.now() / 1000),
            type: tx.type || 'unknown',
            source: 'solscan',
            fee: tx.fee || 0,
            feePayer: tx.feePayer || '',
            slot: tx.slot || 0,
            nativeTransfers: tx.nativeTransfers || [],
            tokenTransfers: tx.tokenTransfers || [],
            accountData: []
          }));
        }
      } catch (endpointError) {
        console.log(`端点 ${url} 失败:`, endpointError.message);
        continue; // 尝试下一个端点
      }
    }
    
    throw new Error('所有 Solscan API 端点都不可用');
  } catch (error) {
    console.error('Solscan API 请求失败:', error);
    throw error;
  }
}


/**
 * 获取代币小数位数（使用 Helius API）
 * @param {string} apiKey - Helius API Key
 * @param {string} tokenMintAddress - 代币的 mint 地址
 * @returns {Promise<number>} 代币小数位数（默认 9）
 */
async function getTokenDecimals(apiKey, tokenMintAddress) {
  try {
    const heliusRpcUrl = `https://mainnet.helius-rpc.com/?api-key=${apiKey}`;
    
    const mintInfo = await fetch(heliusRpcUrl, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: '1',
        method: 'getAccountInfo',
        params: [tokenMintAddress, { encoding: 'jsonParsed' }]
      })
    }).then(r => r.json());
    
    const decimals = mintInfo.result?.value?.data?.parsed?.info?.decimals || 9;
    return decimals;
  } catch (error) {
    console.error(`获取代币 ${tokenMintAddress} 小数位数失败:`, error.message);
    return 9; // 默认返回 9
  }
}

/**
 * 使用 Helius DAS API 的 getTokenAccounts 方法获取代币持有人列表（支持分页）
 * 文档: https://www.helius.dev/docs/api-reference/das/gettokenaccounts
 * 
 * @param {string} apiKey - Helius API Key
 * @param {string} tokenMintAddress - 代币的 mint 地址
 * @returns {Promise<Array>} 持有人列表
 */
/**
 * 使用 Helius DAS API 的 getTokenAccounts 方法获取代币持有人列表（支持分页）
 * 文档: https://www.helius.dev/docs/api-reference/das/gettokenaccounts
 * 
 * 注意：getTokenAccounts 默认按资产 ID 的二进制顺序排序，不支持按余额排序
 * 如果设置了 limit，获取的是前 N 个账户（按资产 ID 排序），而不是余额最大的 N 个
 * 如果需要按余额排序，需要获取所有账户后，在代码中按余额排序
 * 
 * @param {string} apiKey - Helius API Key
 * @param {string} tokenMintAddress - 代币的 mint 地址
 * @param {number} decimals - 代币小数位数（可选，如果传入可避免重复调用）
 * @param {number} limit - 需要获取的账户数量限制（可选，默认获取所有）
 *                       注意：如果设置了 limit，获取的是前 N 个账户（按资产 ID 排序），
 *                       不是余额最大的 N 个。获取后会在代码中按余额排序。
 * @param {number} offset - 偏移量（可选，默认 0，注意：Helius API 不支持 offset，需要通过 cursor 跳过）
 * @returns {Promise<Array>} 持有人列表（已按余额从高到低排序）
 */
export async function getTokenHoldersViaHeliusV2(apiKey, tokenMintAddress, decimals = null, limit = null, offset = 0) {
  try {
    const heliusRpcUrl = `https://mainnet.helius-rpc.com/?api-key=${apiKey}`;

    // 如果没有传入 decimals，则获取代币小数位数
    // 如果调用方已经获取了 decimals（比如从 getTokenTotalSupply），可以传入以避免重复调用
    let tokenDecimals = decimals;
    if (tokenDecimals === null || tokenDecimals === undefined) {
      tokenDecimals = await getTokenDecimals(apiKey, tokenMintAddress);
    }
    
    let allTokenAccounts = [];
    let cursor = null;
    let pageCount = 0;
    const maxPages = 100; // 最多查询 100 页，防止无限循环
    const maxApiLimit = 1000; // Helius API 的最大 limit
    
    // 注意：Helius API 按资产 ID 排序，不支持按余额排序
    // 如果设置了 limit，为了获取余额最大的账户，我们需要：
    // 1. 如果 limit 较小（<= 1000），可以尝试只获取 limit 数量的账户，然后排序（可能不准确）
    // 2. 如果 limit 较大或未设置，获取所有账户后排序（更准确但更慢）
    // 为了准确性，如果设置了 limit，我们仍然获取所有账户，然后排序取前 N 个
    const pageLimit = maxApiLimit; // 每页获取最大数量，确保获取所有数据以便准确排序
    let skippedCount = 0; // 已跳过的账户数量（用于实现 offset）
    
    do {
      pageCount++;
      if (pageCount > maxPages) {
        console.warn(`已达到最大页数限制（${maxPages}），停止查询`);
        break;
      }
      
      // 计算本次请求需要获取的数量
      // 注意：由于 Helius API 按资产 ID 排序，不支持按余额排序
      // 为了获取余额最大的账户，我们需要获取所有账户，然后排序
      // 所以这里不根据 limit 限制请求数量，而是获取所有账户
      let requestLimit = pageLimit;
      
      // 如果设置了 limit，理论上可以只获取 limit 数量的账户
      // 但由于 API 按资产 ID 排序，获取的前 N 个可能不是余额最大的
      // 为了准确性，我们仍然获取所有账户，然后在排序后应用 limit
      
      const requestBody = {
        jsonrpc: '2.0',
        id: '1',
        method: 'getTokenAccounts',
        params: {
          mint: tokenMintAddress,
          limit: requestLimit,
        },
      };
      
      // 如果 cursor 存在，添加到参数中
      if (cursor) {
        requestBody.params.cursor = cursor;
      }
      
      console.log(`第 ${pageCount} 页请求参数:`, JSON.stringify({
        method: requestBody.method,
        mint: requestBody.params.mint,
        limit: requestBody.params.limit,
        cursor: cursor || 'null (第一页)',
        userLimit: limit,
        userOffset: offset,
        skippedCount: skippedCount
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
      
        console.log(`第 ${pageCount} 页响应:`, JSON.stringify({
        hasError: !!result.error,
        error: result.error,
        hasResult: !!result.result,
        total: result.result?.total,
        limit: result.result?.limit,
        tokenAccountsCount: result.result?.token_accounts?.length || 0,
        hasCursor: !!result.result?.cursor,
        lastIndexedSlot: result.result?.last_indexed_slot
      }, null, 2));
      
      if (result.error) {
        throw new Error(`Helius API 错误: ${result.error.message}`);
      }
      
      // getTokenAccounts 返回格式：{ token_accounts: [...], total: number, cursor: "..." }
      const tokenAccounts = result.result?.token_accounts || [];
      cursor = result.result?.cursor || null;
      const total = result.result?.total || 0;
      
      if (!Array.isArray(tokenAccounts)) {
        console.error('返回结果不是数组:', result.result);
        throw new Error(`Helius API 返回格式不正确: 期望数组，得到 ${typeof tokenAccounts}`);
      }
      
      // 处理 offset：跳过前面的账户
      if (offset > 0 && skippedCount < offset) {
        const needToSkip = offset - skippedCount;
        if (tokenAccounts.length <= needToSkip) {
          // 这一页的所有账户都需要跳过
          skippedCount += tokenAccounts.length;
          console.log(`第 ${pageCount} 页：跳过 ${tokenAccounts.length} 个账户（offset 处理），累计跳过 ${skippedCount} 个账户`);
        } else {
          // 跳过部分账户，保留剩余的
          skippedCount += needToSkip;
          const remainingAccounts = tokenAccounts.slice(needToSkip);
          allTokenAccounts = allTokenAccounts.concat(remainingAccounts);
          console.log(`第 ${pageCount} 页：跳过 ${needToSkip} 个账户，保留 ${remainingAccounts.length} 个账户，累计 ${allTokenAccounts.length} 个账户`);
        }
      } else {
        // 不需要跳过，直接添加
        allTokenAccounts = allTokenAccounts.concat(tokenAccounts);
        console.log(`第 ${pageCount} 页：获取到 ${tokenAccounts.length} 个账户，累计 ${allTokenAccounts.length} 个账户，总计 ${total} 个账户，cursor: ${cursor || 'null'}`);
      }
      
      // 注意：由于 Helius API 按资产 ID 排序，不支持按余额排序
      // 为了获取余额最大的账户，我们需要获取所有账户，然后排序
      // 所以这里不根据 limit 提前停止，而是继续获取所有账户
      // limit 会在排序后应用（见代码末尾）
      
      // 如果没有 cursor 或已经获取了所有账户，停止分页
      if (!cursor || allTokenAccounts.length >= total) {
        console.log(`分页结束：cursor=${cursor}, 已获取=${allTokenAccounts.length}, 总计=${total}`);
        break;
      }
      
    } while (cursor);
    
    if (allTokenAccounts.length === 0) {
      throw new Error(`未找到任何代币账户`);
    }
    
    console.log(`✓ 成功获取 ${allTokenAccounts.length} 个代币账户`);
    
    // 按 owner 地址合并余额（因为一个 owner 可能有多个 token account）
    const holderMap = new Map();
    
    for (const tokenAccount of allTokenAccounts) {
      try {
        const owner = tokenAccount.owner;
        const amount = BigInt(tokenAccount.amount || 0);
        const uiAmount = Number(amount) / Math.pow(10, tokenDecimals);
        
        // 只处理余额大于 0 的账户
        if (uiAmount > 0) {
          if (holderMap.has(owner)) {
            // 合并同一 owner 的多个 token account 余额
            const existing = holderMap.get(owner);
            const existingAmount = BigInt(existing.totalAmount);
            const newAmount = existingAmount + amount;
            existing.totalAmount = newAmount.toString();
            existing.uiAmount = Number(newAmount) / Math.pow(10, tokenDecimals);
            existing.tokenAccounts.push(tokenAccount.address);
          } else {
            // 新的 owner
            holderMap.set(owner, {
              address: owner,
              totalAmount: amount.toString(),
              decimals: tokenDecimals,
              uiAmount: uiAmount,
              tokenAccounts: [tokenAccount.address]
            });
          }
        }
      } catch (error) {
        console.error(`处理账户 ${tokenAccount.address} 失败:`, error);
        continue;
      }
    }
    
    // 转换为数组并按余额从高到低排序
    // 注意：Helius API 返回的数据是按资产 ID 排序的，不是按余额排序
    // 所以我们需要在获取所有数据后，按余额重新排序
    const holders = Array.from(holderMap.values())
      .sort((a, b) => b.uiAmount - a.uiAmount);
    
    console.log(`✓ 成功解析 ${holders.length} 个唯一持有人（从 ${allTokenAccounts.length} 个账户中）`);
    console.log(`注意：Helius API 返回的数据是按资产 ID 排序的，已重新按余额排序`);
    
    // 如果设置了 limit，在排序后应用 limit（确保返回的是余额最大的账户）
    if (limit !== null && holders.length > limit) {
      return holders.slice(0, limit);
    }
    
    return holders;
    
  } catch (error) {
    console.error('使用 Helius DAS API getTokenAccounts 获取持有人失败:', error);
    throw error;
  }
}

/**
 * 使用 Helius 增强 RPC 获取代币持有人列表（需要 API Key）
 * 文档: https://www.helius.dev/docs/das-api
 * 注意：Helius 建议使用 getTokenLargestAccounts 获取前 20 个最大持有人
 * 文档: https://www.helius.dev/docs/faqs/das-api
 * 
 * @param {string} apiKey - Helius API Key
 * @param {string} tokenMintAddress - 代币的 mint 地址
 * @param {number} limit - 返回数量限制（getTokenLargestAccounts 最多返回 20 个）
 * @returns {Promise<Array>} 持有人列表
 */
export async function getTokenHoldersViaHelius(apiKey, tokenMintAddress, limit = 100) {
  try {
    const heliusRpcUrl = `https://mainnet.helius-rpc.com/?api-key=${apiKey}`;
    
    // getProgramAccounts 和 getAccountInfo 都是标准 Solana RPC 方法，优先使用 dRPC（如果配置了）
    // 如果没有 dRPC，再使用 Helius
    const drpcUrl = config.drpc.apiKey 
      ? `https://lb.drpc.live/solana/${config.drpc.apiKey}` 
      : null;

    // 注意：某些 RPC 提供商（如 dRPC）可能对 getProgramAccounts 的返回数量有限制
    // 如果使用 dRPC 且返回数量较少，建议切换到 Helius 或使用其他方法
    const rpcUrl = drpcUrl ? drpcUrl : heliusRpcUrl;
    
    const rpcProvider = drpcUrl ? 'dRPC' : 'Helius';
    console.log(`使用 ${rpcProvider} 的 getProgramAccounts 方法获取持有人...`);
    console.log(`注意：某些 RPC 提供商可能对返回数量有限制，如果结果不完整，建议切换到 Helius 或使用其他 RPC 提供商`);

    // 尝试两种方式：base58 字符串（标准）和 base64 编码的字节
    // 同时支持标准 Token 程序和 Token-2022 程序
    const TOKEN_PROGRAM_ID = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA';
    const TOKEN_2022_PROGRAM_ID = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
    
    // 准备两种编码方式的 mint 地址
    let mintBytesBase58 = tokenMintAddress; // base58 字符串（标准方式）
    let mintBytesBase64 = null;
    try {
      const mintPublicKey = new PublicKey(tokenMintAddress);
      const mintBuffer = mintPublicKey.toBuffer();
      mintBytesBase64 = Buffer.from(mintBuffer).toString('base64');
    } catch (error) {
      console.warn('转换 mint 地址为 base64 失败:', error);
    }

    // 尝试查询：先使用标准 Token 程序 + base58，如果失败则尝试其他组合
    let data = null;
    let lastError = null;
    
    const tryQuery = async (programId, bytesFormat, encoding = 'base64', withContext = false) => {
      // 构建请求参数
      const params = [
        programId,
        {
          filters: [
            {
              dataSize: 165, // Token Account 数据大小
            },
            {
              memcmp: {
                offset: 0, // mint 地址在账户数据中的偏移量
                bytes: bytesFormat, // base58 字符串或 base64 编码的字节
              },
            },
          ],
          encoding: encoding,
        },
      ];

      // 如果 withContext 为 true，添加 withContext 参数以获取更多结果
      // 某些 RPC 提供商支持 withContext 来获取更多数据
      if (withContext) {
        params[1].withContext = true;
      }

      const response = await fetch(rpcUrl, {
              method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
              body: JSON.stringify({
                jsonrpc: '2.0',
          id: '1',
          method: 'getProgramAccounts',
          params: params,
        }),
      });
      
      if (!response.ok) {
        throw new Error(`${rpcProvider} API 返回错误: ${response.status}`);
      }
      
      const result = await response.json();
      
      if (result.error) {
        throw new Error(`${rpcProvider} API 错误: ${result.error.message}`);
      }
      
      // 处理 withContext 返回格式
      const accounts = result.result?.context ? result.result.value : result.result;
      
      if (!accounts || !Array.isArray(accounts)) {
        throw new Error(`${rpcProvider} API 返回格式不正确`);
      }
      
      return { result: accounts, hasMore: false }; // 暂时标记为没有更多数据
    };

    // 尝试顺序：
    // 1. 标准 Token 程序 + base58 字符串（最常用）
    // 2. 标准 Token 程序 + base64 字节
    // 3. Token-2022 程序 + base58 字符串
    // 4. Token-2022 程序 + base64 字节
    
    const attempts = [
      { programId: TOKEN_PROGRAM_ID, bytes: mintBytesBase58, encoding: 'base64', desc: '标准 Token 程序 + base58' },
      { programId: TOKEN_PROGRAM_ID, bytes: mintBytesBase64, encoding: 'base64', desc: '标准 Token 程序 + base64' },
      { programId: TOKEN_2022_PROGRAM_ID, bytes: mintBytesBase58, encoding: 'base64', desc: 'Token-2022 程序 + base58' },
      { programId: TOKEN_2022_PROGRAM_ID, bytes: mintBytesBase64, encoding: 'base64', desc: 'Token-2022 程序 + base64' },
    ].filter(attempt => attempt.bytes !== null); // 过滤掉 null 值

    // 首先尝试不使用 withContext，如果结果数量较少，再尝试使用 withContext
    for (const attempt of attempts) {
      try {
        console.log(`尝试使用 ${attempt.desc} 查询...`);
        let queryResult = await tryQuery(attempt.programId, attempt.bytes, attempt.encoding, false);
        console.log(`使用 ${attempt.desc} 查询到 ${queryResult.result.length} 个账户`);
        
        // 如果返回的账户数量较少（可能是 RPC 限制），尝试使用 withContext
        if (queryResult.result.length > 0 && queryResult.result.length < 100) {
          console.log(`返回账户数量较少（${queryResult.result.length}），尝试使用 withContext 获取更多...`);
          try {
            const queryResultWithContext = await tryQuery(attempt.programId, attempt.bytes, attempt.encoding, true);
            if (queryResultWithContext.result.length > queryResult.result.length) {
              console.log(`使用 withContext 获取到 ${queryResultWithContext.result.length} 个账户（增加了 ${queryResultWithContext.result.length - queryResult.result.length} 个）`);
              queryResult = queryResultWithContext;
            }
          } catch (contextError) {
            console.warn(`使用 withContext 查询失败，使用原始结果:`, contextError.message);
          }
        }
        
        // 只有当查询到至少 1 个账户时，才认为成功
        if (queryResult.result.length > 0) {
          data = queryResult;
          console.log(`✓ 成功使用 ${attempt.desc} 查询到 ${data.result.length} 个账户`);
          
          // 如果使用 dRPC 且返回数量较少，尝试使用 Helius 作为备选
          if (drpcUrl && data.result.length < 100) {
            console.log(`\n⚠️  dRPC 只返回了 ${data.result.length} 个账户，尝试使用 Helius 获取更多账户...`);
            try {
              // 临时切换到 Helius RPC URL
              const originalRpcUrl = rpcUrl;
              const originalRpcProvider = rpcProvider;
              
              // 使用 Helius 重试查询
              const heliusQueryResult = await (async () => {
                const response = await fetch(heliusRpcUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: '1',
        method: 'getProgramAccounts',
        params: [
                      attempt.programId,
          {
            filters: [
              {
                            dataSize: 165,
              },
              {
                memcmp: {
                              offset: 0,
                              bytes: attempt.bytes,
                },
              },
            ],
                        encoding: attempt.encoding,
          },
        ],
      }),
    });
    
    if (!response.ok) {
      throw new Error(`Helius API 返回错误: ${response.status}`);
    }
    
                const result = await response.json();
                
                if (result.error) {
                  throw new Error(`Helius API 错误: ${result.error.message}`);
                }
                
                const accounts = result.result?.context ? result.result.value : result.result;
                
                if (!accounts || !Array.isArray(accounts)) {
                  throw new Error(`Helius API 返回格式不正确`);
                }
                
                return { result: accounts };
              })();
              
              if (heliusQueryResult.result.length > data.result.length) {
                console.log(`✓ Helius 返回了 ${heliusQueryResult.result.length} 个账户（比 dRPC 多 ${heliusQueryResult.result.length - data.result.length} 个）`);
                data = heliusQueryResult;
              } else {
                console.log(`Helius 返回了 ${heliusQueryResult.result.length} 个账户，与 dRPC 相同或更少`);
              }
            } catch (heliusError) {
              console.warn(`使用 Helius 备选方案失败:`, heliusError.message);
            }
          }
          
          // 如果返回的账户数量较少，给出警告
          if (data.result.length < 100) {
            console.warn(`⚠️  警告：只查询到 ${data.result.length} 个账户，可能受到 RPC 提供商的限制。`);
            console.warn(`   如果代币实际持有人数量更多，建议：`);
            if (drpcUrl) {
              console.warn(`   1. 当前使用 dRPC，建议取消配置 DRPC_API_KEY 以使用 Helius`);
              console.warn(`   2. Helius 对 getProgramAccounts 进行了优化，通常能返回更多结果`);
            } else {
              console.warn(`   1. 当前使用 Helius，如果仍然受限，可能是代币持有人确实较少`);
              console.warn(`   2. 或考虑使用其他 RPC 提供商（如 QuickNode、Alchemy）`);
            }
            console.warn(`   3. 或使用第三方 API 服务（如 Solscan、Birdeye）获取完整数据`);
          }
          
          break; // 成功则跳出循环
        } else {
          console.warn(`${attempt.desc} 查询返回 0 个账户，继续尝试其他方式...`);
          data = null; // 重置 data，继续尝试
          continue; // 继续尝试下一个
        }
      } catch (error) {
        console.warn(`${attempt.desc} 查询失败:`, error.message);
        lastError = error;
        data = null; // 重置 data
        continue; // 继续尝试下一个
      }
    }

    // 如果所有尝试都失败或都返回 0 个账户
    if (!data || data.result.length === 0) {
      throw new Error(`所有查询方式都失败或返回空结果。最后错误: ${lastError?.message || '所有方式都返回 0 个账户'}`);
    }
    
    // 获取代币小数位数（使用 Helius API，因为 dRPC 可能不支持或失败）
    const decimals = await getTokenDecimals(apiKey, tokenMintAddress);
    
    // 解析所有账户数据，并按 owner 地址合并余额
    const holderMap = new Map(); // key: owner address, value: { address, totalAmount, uiAmount, decimals, tokenAccounts }
    
    for (const account of data.result) {
      try {
        // 解析 base64 编码的账户数据
        const accountData = Buffer.from(account.account.data[0], 'base64');
        
        // owner 在偏移量 32 的位置（32 字节）
        const ownerBuffer = accountData.slice(32, 64);
        const owner = new PublicKey(ownerBuffer).toString();
        
        // amount 在偏移量 64 的位置（8 字节，u64）
        const amountBuffer = accountData.slice(64, 72);
        const amount = Buffer.from(amountBuffer).readBigUInt64LE(0);
        const uiAmount = Number(amount) / Math.pow(10, decimals);
        
        if (uiAmount > 0) {
          if (holderMap.has(owner)) {
            // 合并同一 owner 的多个 token account 余额
            const existing = holderMap.get(owner);
            const existingAmount = BigInt(existing.totalAmount);
            const newAmount = existingAmount + amount;
            existing.totalAmount = newAmount.toString();
            existing.uiAmount = Number(newAmount) / Math.pow(10, decimals);
            existing.tokenAccounts.push(account.pubkey);
          } else {
            // 新的 owner
            holderMap.set(owner, {
              address: owner,
              totalAmount: amount.toString(),
              decimals: decimals,
              uiAmount: uiAmount,
              tokenAccounts: [account.pubkey]
            });
          }
        }
      } catch (error) {
        console.error(`解析账户 ${account.pubkey} 失败:`, error);
        continue;
      }
    }
    
    // 转换为数组并按余额从高到低排序
    const holders = Array.from(holderMap.values())
      .sort((a, b) => b.uiAmount - a.uiAmount)
      .slice(0, limit)
      .map((holder, index) => ({
        rank: index + 1,
        address: holder.address,
        amount: holder.totalAmount,
        decimals: holder.decimals,
        uiAmount: holder.uiAmount,
        tokenAccount: holder.tokenAccounts[0], // 返回第一个 token account
        tokenAccountCount: holder.tokenAccounts.length // 如果有多个 token account，记录数量
      }));
    
    return holders;
  } catch (error) {
    console.error('Helius API 获取持有人失败:', error);
    throw error;
  }
}


/**
 * 获取代币的总供应量（Total Supply）和元数据
 * 已移动到 cacheService.js，这里重新导出以保持向后兼容
 * @param {string} apiKey - Helius API Key（可选）
 * @param {string} tokenMintAddress - 代币的 mint 地址
 * @returns {Promise<{supply: string, uiSupply: number, decimals: number, symbol: string, name: string}>} 总供应量信息和元数据
 */
export { getTokenTotalSupply } from '../services/cacheService.js';

/**
 * 解析 Helius API 返回的交易数据
 */
export function parseHeliusTransaction(tx, tokenMint) {
  try {
    const tokenTransfers = tx.tokenTransfers || [];
    const nativeTransfers = tx.nativeTransfers || [];
    
    // 查找涉及目标代币的转移
    const relevantTransfers = tokenTransfers.filter(
      transfer => transfer.mint === tokenMint
    );
    
    if (relevantTransfers.length === 0) {
      return null;
    }
    
    // 计算代币转移总量
    let totalTokenAmount = 0;
    const addresses = new Set();
    
    for (const transfer of relevantTransfers) {
      const amount = parseFloat(transfer.tokenAmount);
      totalTokenAmount += amount;
      addresses.add(transfer.fromUserAccount);
      addresses.add(transfer.toUserAccount);
    }
    
    // 计算 SOL 转移总量
    let totalSolAmount = 0;
    for (const transfer of nativeTransfers) {
      totalSolAmount += Math.abs(transfer.amount / 1e9);
    }
    
    // 判断交易类型
    // 简化判断：如果代币从某个地址转出，可能是卖出；如果转入，可能是买入
    // 实际判断需要结合 DEX 的具体逻辑
    
    const result = {
      signature: tx.signature,
      timestamp: tx.timestamp,
      blockTime: tx.timestamp,
      slot: tx.slot,
      type: totalTokenAmount > 0 ? 'buy' : 'sell',
      price: totalSolAmount > 0 && totalTokenAmount > 0 
        ? totalSolAmount / Math.abs(totalTokenAmount) 
        : null,
      amount: Math.abs(totalTokenAmount),
      solAmount: totalSolAmount,
      addresses: Array.from(addresses),
      source: tx.source,
      fee: tx.fee / 1e9
    };
    
    return result;
  } catch (error) {
    console.error('解析 Helius 交易失败:', error);
    return null;
  }
}

