import { Connection, PublicKey } from '@solana/web3.js';
import { config } from '../config/index.js';

/**
 * 地址聚类服务
 * 用于识别和合并关联地址，类似 Axiom 的功能
 * 
 * 实现方式：
 * 1. 按 Owner 合并（基础，已实现）
 * 2. 交易关系分析（通过分析地址之间的交易识别关联）
 * 3. PDA 识别（程序派生地址）
 * 4. 多签钱包识别
 * 5. 行为模式识别（相似交易模式）
 */

/**
 * 获取地址的交易总数（用于判断是否是机器人）
 * @param {string} address - 地址
 * @param {number} threshold - 阈值，如果交易数达到这个值，认为是机器人
 * @returns {Promise<number>} 交易总数（如果达到阈值，返回阈值+1）
 */
async function getAddressTransactionCount(address, threshold = 500) {
  // 优先使用 dRPC（如果配置了），否则使用其他配置的 RPC
  if (!config.drpc.apiKey && !config.helius.apiKey) {
    throw new Error('需要配置 dRPC 或 Helius API Key 才能使用地址聚类功能');
  }

  const rpcEndpoint = config.drpc.apiKey
    ? `https://lb.drpc.live/solana/${config.drpc.apiKey}`
    : config.solana.rpcEndpoint;

  const connection = new Connection(rpcEndpoint, 'confirmed');

  try {
    const publicKey = new PublicKey(address);
    // 查询交易签名，limit 设置为阈值+1，如果返回的数量达到阈值+1，说明交易很多
    const signatures = await connection.getSignaturesForAddress(publicKey, {
      limit: threshold + 1
    });

    // 如果返回的数量达到阈值+1，说明交易数 >= 阈值+1，认为是机器人
    if (signatures.length >= threshold + 1) {
      return threshold + 1; // 返回阈值+1，表示交易很多
    }

    return signatures.length;
  } catch (error) {
    console.error(`获取地址 ${address} 交易数量失败:`, error.message);
    return 0; // 出错时返回 0，不进行过滤
  }
}

/**
 * 获取地址的交易历史（用于分析关联关系）
 * @param {string} address - 地址
 * @param {number} limit - 限制数量
 * @returns {Promise<Array>} 交易列表
 */
async function getAddressTransactions(address, limit = 50) {
  // 优先使用 dRPC（如果配置了），否则使用其他配置的 RPC
  // getSignaturesForAddress 和 getParsedTransaction 都是标准 Solana RPC 方法，dRPC 支持
  if (!config.drpc.apiKey && !config.helius.apiKey) {
    throw new Error('需要配置 dRPC 或 Helius API Key 才能使用地址聚类功能');
  }

  // 优先使用 dRPC，如果没有则使用配置的 RPC 端点（可能是 Helius 或其他）
  const rpcEndpoint = config.drpc.apiKey
    ? `https://lb.drpc.live/solana/${config.drpc.apiKey}`
    : config.solana.rpcEndpoint;
  
  const rpcProvider = config.drpc.apiKey ? 'dRPC' : (config.helius.apiKey ? 'Helius' : '其他 RPC');
  console.log(`  使用 ${rpcProvider} 解析交易...`);

  const connection = new Connection(
    rpcEndpoint,
    'confirmed'
  );

  try {
    const publicKey = new PublicKey(address);
    const signatures = await connection.getSignaturesForAddress(publicKey, {
      limit: Math.min(limit, 100)
    });

    if (!signatures || signatures.length === 0) {
      return [];
    }

    // 获取交易详情（只获取前几个，避免过多请求）
    const transactions = await Promise.all(
      signatures.slice(0, 10).map(async (sig) => {
        try {
          const tx = await connection.getParsedTransaction(sig.signature, {
            maxSupportedTransactionVersion: 0
          });
          return {
            signature: sig.signature,
            blockTime: sig.blockTime,
            transaction: tx
          };
        } catch (error) {
          return null;
        }
      })
    );

    return transactions.filter(tx => tx !== null);
  } catch (error) {
    console.error(`获取地址 ${address} 的交易历史失败:`, error.message);
    return [];
  }
}

/**
 * 检查交易是否包含代币转移（更相关的交易类型）
 * @param {Object} transaction - 交易对象
 * @returns {boolean} 是否包含代币转移
 */
function hasTokenTransfer(transaction) {
  if (!transaction || !transaction.transaction || !transaction.transaction.meta) {
    return false;
  }

  const meta = transaction.transaction.meta;
  
  // 检查是否有代币余额变化（代币转移）
  if (meta.preTokenBalances && meta.postTokenBalances) {
    if (meta.preTokenBalances.length > 0 || meta.postTokenBalances.length > 0) {
      return true;
    }
  }

  // 检查是否有 SOL 余额变化（SOL 转移）
  if (meta.preBalances && meta.postBalances) {
    for (let i = 0; i < Math.min(meta.preBalances.length, meta.postBalances.length); i++) {
      if (meta.preBalances[i] !== meta.postBalances[i]) {
        return true;
      }
    }
  }

  return false;
}

/**
 * 从交易中提取关联地址
 * 优先关注代币转移相关的交易，这些更能反映地址之间的真实关联
 * @param {Object} transaction - 交易对象
 * @param {string} targetAddress - 目标地址
 * @returns {Array<string>} 关联地址列表
 */
function extractRelatedAddresses(transaction, targetAddress) {
  const relatedAddresses = new Set();
  
  if (!transaction || !transaction.transaction) {
    return [];
  }

  const tx = transaction.transaction;
  const message = tx.message;

  if (!message || !message.accountKeys) {
    return [];
  }

  // 优先关注代币转移交易（更相关）
  // 如果交易不包含代币转移，降低权重（但不完全忽略）
  const hasTransfer = hasTokenTransfer(transaction);
  if (!hasTransfer) {
    // 对于非转移交易（如授权、加池子），降低相关性
    // 但仍然提取地址，因为可能有关联
  }

  // 提取交易中的所有地址
  const accountKeys = message.accountKeys.map(key => 
    typeof key === 'string' ? key : key.pubkey?.toString()
  ).filter(Boolean);

  // 从代币余额变化中提取地址（最相关，因为涉及实际的代币转移）
  if (tx.meta && tx.meta.preTokenBalances && tx.meta.postTokenBalances) {
    const preTokenBalances = tx.meta.preTokenBalances;
    const postTokenBalances = tx.meta.postTokenBalances;

    // 找出代币余额发生变化的账户 owner
    preTokenBalances.forEach(pre => {
      if (pre.owner && pre.owner !== targetAddress) {
        relatedAddresses.add(pre.owner);
      }
    });

    postTokenBalances.forEach(post => {
      if (post.owner && post.owner !== targetAddress) {
        relatedAddresses.add(post.owner);
      }
    });
  }

  // 从 SOL 余额变化中提取地址（也相关）
  if (tx.meta) {
    const preBalances = tx.meta.preBalances || [];
    const postBalances = tx.meta.postBalances || [];
    
    // 找出余额发生显著变化的账户（忽略小额变化，可能是手续费）
    for (let i = 0; i < Math.min(preBalances.length, postBalances.length); i++) {
      const balanceChange = Math.abs(postBalances[i] - preBalances[i]);
      // 只关注大于 0.01 SOL 的变化（忽略手续费等小额变化）
      if (balanceChange > 10000000 && accountKeys[i] && accountKeys[i] !== targetAddress) {
        relatedAddresses.add(accountKeys[i]);
      }
    }
  }

  // 如果代币转移中没有找到关联地址，才考虑其他账户（可能是授权、加池子等）
  if (relatedAddresses.size === 0) {
    // 添加其他账户地址（但权重较低）
    accountKeys.forEach(addr => {
      if (addr !== targetAddress) {
        relatedAddresses.add(addr);
      }
    });
  }

  return Array.from(relatedAddresses);
}

/**
 * 分析地址的关联关系
 * @param {string} address - 地址
 * @param {number} transactionLimit - 分析的交易数量限制
 * @returns {Promise<Object>} 关联地址信息
 */
export async function analyzeAddressRelations(address, transactionLimit = 20) {
  try {
    console.log(`分析地址 ${address} 的关联关系...`);
    
    const transactions = await getAddressTransactions(address, transactionLimit);
    
    if (transactions.length === 0) {
      return {
        address,
        relatedAddresses: [],
        transactionCount: 0,
        clusters: []
      };
    }

    // 优先分析代币转移交易（更相关）
    const transferTransactions = transactions.filter(tx => hasTokenTransfer(tx));
    const otherTransactions = transactions.filter(tx => !hasTokenTransfer(tx));
    
    console.log(`  找到 ${transferTransactions.length} 个代币转移交易，${otherTransactions.length} 个其他交易`);

    // 统计关联地址的出现频率（代币转移交易权重更高）
    const addressFrequency = new Map();
    const addressWeights = new Map(); // 记录每个地址的权重（代币转移 = 2，其他 = 1）
    
    // 先处理代币转移交易（权重 2）
    transferTransactions.forEach(tx => {
      const related = extractRelatedAddresses(tx, address);
      related.forEach(addr => {
        addressFrequency.set(addr, (addressFrequency.get(addr) || 0) + 2);
        addressWeights.set(addr, (addressWeights.get(addr) || 0) + 2);
      });
    });

    // 再处理其他交易（权重 1，但只处理前几个）
    otherTransactions.slice(0, 5).forEach(tx => {
      const related = extractRelatedAddresses(tx, address);
      related.forEach(addr => {
        addressFrequency.set(addr, (addressFrequency.get(addr) || 0) + 1);
        addressWeights.set(addr, (addressWeights.get(addr) || 0) + 1);
      });
    });

    // 按权重排序，找出最相关的地址
    // 使用权重而不是频率，因为代币转移交易更重要
    const totalWeight = transferTransactions.length * 2 + Math.min(otherTransactions.length, 5);
    const sortedRelated = Array.from(addressWeights.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 10) // 只取前 10 个最相关的地址
      .map(([addr, weight]) => ({
        address: addr,
        frequency: addressFrequency.get(addr) || 0,
        weight: weight,
        relevance: totalWeight > 0 ? weight / totalWeight : 0 // 相关性分数（基于权重）
      }));

    return {
      address,
      relatedAddresses: sortedRelated,
      transactionCount: transactions.length,
      clusters: []
    };
  } catch (error) {
    console.error(`分析地址 ${address} 关联关系失败:`, error.message);
    return {
      address,
      relatedAddresses: [],
      transactionCount: 0,
      clusters: [],
      error: error.message
    };
  }
}

/**
 * 批量分析地址关联关系（用于聚类）
 * @param {Array<string>} addresses - 地址列表
 * @param {number} transactionLimit - 每个地址分析的交易数量限制
 * @returns {Promise<Map>} 地址聚类映射 (key: 主地址, value: 关联地址数组)
 */
export async function clusterAddresses(addresses, transactionLimit = 10) {
  console.log(`开始聚类 ${addresses.length} 个地址...`);
  
  const clusterMap = new Map(); // key: 主地址, value: Set of 关联地址
  const addressToCluster = new Map(); // key: 地址, value: 主地址（所属聚类）

  // 初始化：每个地址先作为自己的主地址
  addresses.forEach(addr => {
    clusterMap.set(addr, new Set([addr]));
    addressToCluster.set(addr, addr);
  });

  // 分析每个地址的关联关系
  for (let i = 0; i < addresses.length; i++) {
    const address = addresses[i];
    console.log(`分析地址 ${i + 1}/${addresses.length}: ${address.substring(0, 8)}...`);

    try {
      const relations = await analyzeAddressRelations(address, transactionLimit);
      
      // 找出高度相关的地址（相关性 > 0.3）
      const highlyRelated = relations.relatedAddresses
        .filter(rel => rel.relevance > 0.3)
        .map(rel => rel.address);

      // 如果这些地址也在我们的列表中，将它们合并到同一个聚类
      highlyRelated.forEach(relatedAddr => {
        if (addresses.includes(relatedAddr)) {
          const mainCluster = addressToCluster.get(address);
          const relatedCluster = addressToCluster.get(relatedAddr);

          if (mainCluster !== relatedCluster) {
            // 合并两个聚类
            const mainSet = clusterMap.get(mainCluster);
            const relatedSet = clusterMap.get(relatedCluster);

            // 将 relatedSet 中的所有地址合并到 mainSet
            relatedSet.forEach(addr => {
              mainSet.add(addr);
              addressToCluster.set(addr, mainCluster);
            });

            // 删除旧的聚类
            clusterMap.delete(relatedCluster);
          }
        }
      });

      // 避免请求过快
      await new Promise(resolve => setTimeout(resolve, 200));
    } catch (error) {
      console.error(`分析地址 ${address} 失败:`, error.message);
      continue;
    }
  }

  console.log(`聚类完成，共识别 ${clusterMap.size} 个聚类`);

  return clusterMap;
}

/**
 * 合并关联地址的持仓
 * @param {Array} holders - 持有人列表
 * @param {boolean} enableClustering - 是否启用地址聚类（默认 false，因为较慢）
 * @returns {Promise<Array>} 合并后的持有人列表
 */
export async function mergeClusteredHolders(holders, enableClustering = false) {
  if (!enableClustering || holders.length === 0) {
    // 如果未启用聚类，只按 owner 合并（已有逻辑）
    return holders;
  }

  if (!config.helius.apiKey) {
    console.warn('未配置 Helius API Key，跳过地址聚类，仅按 owner 合并');
    return holders;
  }

  try {
    console.log(`开始地址聚类分析（${holders.length} 个地址）...`);
    
    // 提取所有地址
    const addresses = holders.map(h => h.address);
    
    // 执行聚类（限制分析的地址数量，避免过多请求）
    const maxAddressesToAnalyze = 50; // 只分析前 50 个地址（通常是持仓最多的）
    const addressesToAnalyze = addresses.slice(0, maxAddressesToAnalyze);
    
    // 过滤掉交易次数过多的地址（可能是机器人）
    console.log(`正在检查地址交易次数，过滤机器人地址...`);
    const transactionCountThreshold = 500; // 交易次数阈值，超过此值认为是机器人
    const filteredAddresses = [];
    const filteredRobotAddresses = [];
    
    for (const address of addressesToAnalyze) {
      try {
        const txCount = await getAddressTransactionCount(address, transactionCountThreshold);
        if (txCount >= transactionCountThreshold + 1) {
          // 交易次数过多，可能是机器人，过滤掉
          filteredRobotAddresses.push(address);
          console.log(`  过滤机器人地址: ${address.substring(0, 8)}... (交易数 >= ${transactionCountThreshold + 1})`);
        } else {
          filteredAddresses.push(address);
        }
        // 避免请求过快
        await new Promise(resolve => setTimeout(resolve, 100));
      } catch (error) {
        console.error(`检查地址 ${address} 交易次数失败:`, error.message);
        // 出错时保留地址，不进行过滤
        filteredAddresses.push(address);
      }
    }
    
    if (filteredRobotAddresses.length > 0) {
      console.log(`已过滤 ${filteredRobotAddresses.length} 个机器人地址（交易次数过多）`);
    }
    
    if (filteredAddresses.length === 0) {
      console.log(`所有地址都被过滤为机器人，跳过聚类分析`);
      return holders; // 返回原始数据
    }
    
    console.log(`开始对 ${filteredAddresses.length} 个地址进行聚类分析...`);
    const clusterMap = await clusterAddresses(filteredAddresses, 5); // 每个地址只分析 5 个交易

    // 创建地址到主地址的映射
    const addressToMain = new Map();
    clusterMap.forEach((cluster, mainAddr) => {
      cluster.forEach(addr => {
        addressToMain.set(addr, mainAddr);
      });
    });

    // 合并同一聚类中的地址持仓
    const mergedMap = new Map();

    holders.forEach(holder => {
      const mainAddr = addressToMain.get(holder.address) || holder.address;
      
      if (mergedMap.has(mainAddr)) {
        const existing = mergedMap.get(mainAddr);
        const existingAmount = BigInt(existing.amount || '0');
        const holderAmount = BigInt(holder.amount || '0');
        existing.amount = (existingAmount + holderAmount).toString();
        existing.uiAmount = (existing.uiAmount || 0) + (holder.uiAmount || 0);
        existing.relatedAddresses = existing.relatedAddresses || [];
        if (holder.address !== mainAddr && !existing.relatedAddresses.includes(holder.address)) {
          existing.relatedAddresses.push(holder.address);
        }
        existing.addressCount = (existing.addressCount || 1) + (holder.address !== mainAddr ? 1 : 0);
      } else {
        mergedMap.set(mainAddr, {
          ...holder,
          address: mainAddr, // 使用主地址作为显示地址
          relatedAddresses: holder.address !== mainAddr ? [holder.address] : [],
          addressCount: holder.address !== mainAddr ? 2 : 1
        });
      }
    });

    // 转换为数组并排序
    const merged = Array.from(mergedMap.values())
      .sort((a, b) => b.uiAmount - a.uiAmount)
      .map((holder, index) => ({
        ...holder,
        rank: index + 1
      }));

    console.log(`地址聚类完成：${holders.length} 个地址合并为 ${merged.length} 个聚类`);

    return merged;
  } catch (error) {
    console.error('地址聚类失败:', error.message);
    console.log('回退到基础合并（按 owner）');
    return holders;
  }
}

/**
 * 快速聚类（仅基于已知规则，不分析交易）
 * 适用于大量地址的场景
 * @param {Array} holders - 持有人列表
 * @returns {Array} 合并后的持有人列表
 */
export function quickClusterHolders(holders) {
  // 基础聚类：按 owner 合并（已经在 getTokenHoldersViaHelius 中实现）
  // 这里可以添加其他快速规则，比如：
  // 1. 识别已知的关联地址模式
  // 2. 识别 PDA 地址模式
  // 3. 识别多签钱包模式

  // 目前只返回原列表（因为按 owner 合并已经在 getTokenHoldersViaHelius 中完成）
  return holders;
}

