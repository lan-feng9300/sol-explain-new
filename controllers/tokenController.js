import {
  calculateTransactionStats,
  getAddressTransactionHistory,
  getAddressTransactions,
  getPoolTransactions,
  getTokenTransactions,
  getTransactionsForAddress
} from '../services/tokenService.js';
import {filterLiquidityPoolsAndExchanges} from '../services/tokenSyncService.js';
import {config} from '../config/index.js';
import {getTokenHoldersSummaryByBatchTime} from '../db/solAddrSplInfoMapper.js';
import {getTokenMetadataMultipleViaHelius, getSolUsdPrice} from '../services/cacheService.js';
import {batchSaveTradeInfo, getRobotAddresses} from '../db/solTradeInfoMapper.js';
import {
  checkRPCConfig,
  parseMultipleTradeInfo,
  parseMultipleTradeInfoFromTransactions,
  parseTransactionBuySellInfo
} from '../services/transactionParseService.js';

/**
 * 获取代币交易记录
 */
export async function getTokenTransactionsHandler(req, res) {
  try {
    const { tokenAddress } = req.params;
    const limit = parseInt(req.query.limit) || 100;
    const useHelius = req.query.useHelius === 'true' && config.helius.apiKey;
    
    console.log(`正在获取代币 ${tokenAddress} 的交易记录...`);
    
    let transactions = [];
    
    try {
      transactions = await getTokenTransactions(tokenAddress, limit, useHelius);
      
      if (transactions.length === 0 && !useHelius) {
        throw new Error('未找到交易记录');
      }
    } catch (error) {
      console.error('获取交易记录失败:', error.message);
      
      // 如果 Solscan 也失败，返回友好的错误提示
      return res.json({
        success: false,
        message: '无法获取交易记录',
        error: '此功能需要配置 Helius API Key 或提供交易对地址',
        solutions: [
          {
            method: '使用 Helius API（推荐）',
            steps: [
              '1. 访问 https://www.helius.dev/ 注册账号',
              '2. 获取免费的 API Key',
              '3. 设置环境变量: export HELIUS_API_KEY=your_api_key',
              '4. 重新启动服务器'
            ]
          },
          {
            method: '使用交易对地址',
            steps: [
              '1. 在 Solscan 上找到代币的交易对地址（Pool Address）',
              '2. 使用 /api/pool/:poolAddress 端点查询',
              '3. 例如: /api/pool/YOUR_POOL_ADDRESS'
            ]
          }
        ],
        hint: '或者访问 https://solscan.io/token/' + tokenAddress + ' 查看交易记录'
      });
    }
    
    // 统计信息
    const stats = calculateTransactionStats(transactions);
    
    res.json({
      success: true,
      tokenAddress,
      stats,
      transactions
    });
  } catch (error) {
    console.error('获取交易记录失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

/**
 * 获取交易对交易记录
 */
export async function getPoolTransactionsHandler(req, res) {
  try {
    const { poolAddress } = req.params;
    const limit = parseInt(req.query.limit) || 100;
    
    const transactions = await getPoolTransactions(poolAddress, limit);
    
    res.json({
      success: true,
      poolAddress,
      transactionCount: transactions.length,
      transactions
    });
  } catch (error) {
    console.error('获取交易对交易记录失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

/**
 * 获取地址交易记录
 */
export async function getAddressTransactionsHandler(req, res) {
  try {
    const { address } = req.params;
    const limit = parseInt(req.query.limit) || 50;
    
    const transactions = await getAddressTransactions(address, limit);
    
    res.json({
      success: true,
      address,
      transactionCount: transactions.length,
      transactions
    });
  } catch (error) {
    console.error('获取地址交易记录失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

/**
 * 获取代币持有人列表（使用 Helius DAS API getTokenAccounts）
 * 文档: https://www.helius.dev/docs/api-reference/das/gettokenaccounts
 */
export async function getTokenHoldersHandler(req, res) {
  try {
    const { tokenAddress } = req.params;
    const offset = parseInt(req.query.offset) || 0;
    const limit = parseInt(req.query.limit) || 100;
    const filterPools = req.query.filterPools === 'true'; // 默认 false，需要显式启用
    const checkOnChain = req.query.checkOnChain === 'true'; // 默认 false，需要显式启用
    const enableClustering = req.query.enableClustering === 'true'; // 默认 false，地址聚类较慢
    
    if (!config.helius.apiKey) {
      return res.status(400).json({
        success: false,
        error: '未配置 Helius API Key',
        hint: '请在 .env 文件中设置 HELIUS_API_KEY',
        registerUrl: 'https://www.helius.dev/'
      });
    }

    // 先获取代币元数据（包含 symbol、supply、decimals），然后传递给 getTokenHoldersViaHeliusV2 以避免重复调用
    // 使用缓存服务，先从 Redis 获取，取不到才调用 API
    console.log(`正在获取代币元数据（使用缓存）...`);
    let totalSupplyInfo;
    let decimals = null;
    try {
      const { getTokenMetadataViaHelius } = await import('../services/cacheService.js');
      const metadata = await getTokenMetadataViaHelius(
        config.helius.apiKey,
        tokenAddress
      );
      
      if (metadata) {
        decimals = metadata.decimals;
        const supply = metadata.supply || '0';
        const uiSupply = Number(supply) / Math.pow(10, decimals);
        
        totalSupplyInfo = {
          supply: supply,
          uiSupply: uiSupply,
          decimals: decimals,
          symbol: metadata.symbol || '',
          name: '' // 元数据方法不返回 name
        };
        
        console.log(`代币符号: ${totalSupplyInfo.symbol || '(未获取)'}, 总供应量: ${totalSupplyInfo.uiSupply}, decimals: ${decimals}`);
      } else {
        throw new Error('元数据返回为空');
      }
    } catch (error) {
      console.warn(`获取代币元数据失败: ${error.message}，将在获取持有人时获取 decimals`);
      // 如果获取失败，decimals 保持为 null，让 getTokenHoldersViaHeliusV2 内部获取
    }

    // 使用 getTokenAccounts 方法获取持有人（传入 decimals、limit 和 offset 以避免重复调用）
    // 注意：Helius API 不支持 offset，但我们可以通过跳过前面的账户来实现
    // 为了性能，如果 offset 很大，建议先获取所有数据再分页
    const { getTokenHoldersViaHeliusV2 } = await import('../src/tokenTracker.js');
    const holders = await getTokenHoldersViaHeliusV2(
      config.helius.apiKey,
      tokenAddress,
      decimals, // 传入已获取的 decimals（如果为 null，函数内部会获取）
      limit,    // 传入 limit 参数，让 API 只获取需要的数量
      offset    // 传入 offset 参数（通过跳过前面的账户实现）
    );

    if (!holders || holders.length === 0) {
      return res.json({
        success: true,
        tokenAddress,
        totalHolders: 0,
        message: '未找到持有人',
        holders: []
      });
    }

    // 如果之前获取总供应量失败，现在从 holders 中获取 decimals 并重新计算
    if (!totalSupplyInfo) {
      const calculatedSupply = holders.reduce((sum, holder) => sum + (holder.uiAmount || 0), 0);
      const holderDecimals = holders.length > 0 ? holders[0].decimals : 9;
      totalSupplyInfo = {
        supply: '0',
        uiSupply: calculatedSupply,
        decimals: holderDecimals,
        symbol: '',
        name: '',
      };
      console.log(`使用计算的总供应量: ${calculatedSupply}`);
    }

    // 过滤流动性池和交易所地址
    let { filteredHolders, filteredCount } = await filterLiquidityPoolsAndExchanges(
      holders,
      filterPools,
      checkOnChain,
      console.log
    );

    // 应用地址聚类（如果启用，在过滤流动池之后执行）
    if (enableClustering && filteredHolders.length > 0) {
      console.log(`启用地址聚类分析（在过滤流动池之后）...`);
      try {
        const { mergeClusteredHolders } = await import('../services/addressClusteringService.js');
        filteredHolders = await mergeClusteredHolders(filteredHolders, true);
        console.log(`地址聚类完成，剩余 ${filteredHolders.length} 个聚类后的地址`);
      } catch (error) {
        console.warn(`地址聚类失败: ${error.message}，继续使用未聚类的数据`);
      }
    }

    // 计算百分比并格式化持有人数据
    console.log(`正在格式化持有人数据...`);
    const totalSupply = totalSupplyInfo.uiSupply;
    const tokenSymbol = totalSupplyInfo.symbol || '';

    const formattedHolders = filteredHolders.map((holder, index) => ({
      rank: holder.rank !== null ? holder.rank : index + 1,
      address: holder.address,
      amount: holder.totalAmount || holder.amount, // 兼容两种数据结构
      uiAmount: holder.uiAmount,
      percentage: totalSupply > 0 ? ((holder.uiAmount / totalSupply) * 100).toFixed(4) : '0.0000',
    }));

    // 注意：offset 和 limit 已经在 getTokenHoldersViaHeliusV2 中处理了
    // 这里直接使用 formattedHolders（已经应用了 offset 和 limit）
    const paginatedHolders = formattedHolders;

    // 保存到数据库
    console.log(`正在保存 ${paginatedHolders.length} 条数据到数据库...`);
    try {
      const { insertTokenHolders } = await import('../db/solAddrSplInfoMapper.js');
      await insertTokenHolders(paginatedHolders, tokenAddress, tokenSymbol);
      console.log(`数据保存完成`);
    } catch (error) {
      console.warn(`保存数据到数据库失败: ${error.message}`);
    }
    
    res.json({
      success: true,
      tokenAddress,
      totalHolders: filteredHolders.length, // 过滤和聚类后的总数量
      filtered: filteredCount, // 过滤掉的流动性池/交易所地址数量
      totalSupply: totalSupply,
      totalSupplyRaw: totalSupplyInfo.supply,
      decimals: totalSupplyInfo.decimals,
      offset,
      limit,
      holders: paginatedHolders, // 分页后的持有人列表
      clustered: enableClustering, // 是否启用了地址聚类
      method: 'Helius DAS API getTokenAccounts',
      provider: 'Helius'
    });
  } catch (error) {
    console.error('获取代币持有人失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      hint: '请确保代币地址正确，或尝试使用 Solscan 查看: https://solscan.io/token/' + req.params.tokenAddress,
      suggestion: '此方法使用 Helius DAS API getTokenAccounts，支持分页获取所有账户'
    });
  }
}

export async function parseTransactionBuySellInfoHandler(req, res) {
  try {
    const { signature } = req.params;
    
    // 参数验证
    if (!signature) {
      return res.status(400).json({
        success: false,
        error: '缺少交易签名参数'
      });
    }
    
    // 检查 RPC 配置
    if (!checkRPCConfig()) {
      return res.status(400).json({
        success: false,
        error: '需要配置 dRPC 或 Helius API Key 才能使用此功能',
        hint: '请在 .env 文件中设置 DRPC_API_KEY（优先）或 HELIUS_API_KEY'
      });
    }
    
    // 获取完整交易详情用于详细分析
    const rpcEndpoint = config.drpc.apiKey
      ? `https://lb.drpc.live/solana/${config.drpc.apiKey}`
      : config.solana.rpcEndpoint;
    const { Connection } = await import('@solana/web3.js');
    const connection = new Connection(rpcEndpoint, 'confirmed');
    
    // 获取完整交易数据
    const transaction = await connection.getParsedTransaction(signature, {
      maxSupportedTransactionVersion: 0
    });
    
    if (!transaction) {
      return res.status(404).json({
        success: false,
        error: '交易不存在或无法获取'
      });
    }
    
    // 提取交易发起者（签名者）
    const accountKeys = transaction.transaction?.message?.accountKeys || [];
    const signers = accountKeys
      .map((acc, index) => {
        const isSigner = acc.signer || false;
        const address = typeof acc === 'string' ? acc : (acc.pubkey?.toString() || acc.toString());
        return { address, isSigner, index };
      })
      .filter(acc => acc.isSigner);
    
    // 提取所有账户地址
    const allAccounts = accountKeys.map((acc, index) => {
      const address = typeof acc === 'string' ? acc : (acc.pubkey?.toString() || acc.toString());
      return {
        address,
        isSigner: acc.signer || false,
        isWritable: acc.writable || false,
        index
      };
    });
    
    // 提取指令信息
    const instructions = transaction.transaction?.message?.instructions || [];
    const programInteractions = instructions.map((ix, idx) => {
      const programId = ix.programId?.toString() || 'Unknown';
      return {
        index: idx,
        programId,
        programName: programId,
        parsed: ix.parsed || null
      };
    });
    
    // 提取余额变化
    const meta = transaction.meta || {};
    const preBalances = meta.preBalances || [];
    const postBalances = meta.postBalances || [];
    const balanceChanges = accountKeys.map((acc, index) => {
      const address = typeof acc === 'string' ? acc : (acc.pubkey?.toString() || acc.toString());
      const preBalance = preBalances[index] || 0;
      const postBalance = postBalances[index] || 0;
      const change = (postBalance - preBalance) / 1e9; // 转换为 SOL
      return {
        address,
        isSigner: acc.signer || false,
        preBalance: preBalance / 1e9,
        postBalance: postBalance / 1e9,
        change,
        changeLamports: postBalance - preBalance
      };
    }).filter(change => change.changeLamports !== 0);
    
    // 提取代币余额变化
    const preTokenBalances = meta.preTokenBalances || [];
    const postTokenBalances = meta.postTokenBalances || [];
    const tokenBalanceChanges = [];
    
    // 合并前后代币余额，找出变化
    const tokenBalanceMap = new Map();
    preTokenBalances.forEach(balance => {
      const key = `${balance.accountIndex}_${balance.mint}`;
      tokenBalanceMap.set(key, {
        accountIndex: balance.accountIndex,
        mint: balance.mint,
        owner: balance.owner,
        preAmount: balance.uiTokenAmount?.uiAmount || 0,
        decimals: balance.uiTokenAmount?.decimals || 0
      });
    });
    
    postTokenBalances.forEach(balance => {
      const key = `${balance.accountIndex}_${balance.mint}`;
      const existing = tokenBalanceMap.get(key) || {
        accountIndex: balance.accountIndex,
        mint: balance.mint,
        owner: balance.owner,
        preAmount: 0,
        decimals: balance.uiTokenAmount?.decimals || 0
      };
      const postAmount = balance.uiTokenAmount?.uiAmount || 0;
      const change = postAmount - existing.preAmount;
      
      if (change !== 0) {
        const account = accountKeys[balance.accountIndex];
        const address = typeof account === 'string' ? account : (account?.pubkey?.toString() || account?.toString() || `Account${balance.accountIndex}`);
        tokenBalanceChanges.push({
          accountIndex: balance.accountIndex,
          address,
          mint: balance.mint,
          owner: balance.owner,
          preAmount: existing.preAmount,
          postAmount,
          change,
          decimals: balance.uiTokenAmount?.decimals || 0
        });
      }
    });
    
    // 调用 service 处理业务逻辑（获取买卖信息）
    let tradeInfo = null;
    try {
      const tradeResult = await parseTransactionBuySellInfo(signature);
      if (tradeResult.success) {
        tradeInfo = tradeResult.data;
      }
    } catch (error) {
      console.warn('解析买卖信息失败:', error.message);
    }
    
    // 组合返回结果
    const result = {
      success: true,
      signature,
      transactionInfo: {
        slot: transaction.slot,
        blockTime: transaction.blockTime ? new Date(transaction.blockTime * 1000).toISOString() : null,
        fee: meta.fee ? meta.fee / 1e9 : 0, // 转换为 SOL
        status: transaction.meta?.err ? 'failed' : 'success',
        error: transaction.meta?.err || null
      },
      signers: signers.map(s => s.address),
      primarySigner: signers.length > 0 ? signers[0].address : null,
      allAccounts: allAccounts.map(a => ({
        address: a.address,
        isSigner: a.isSigner,
        isWritable: a.isWritable
      })),
      programInteractions,
      balanceChanges: balanceChanges.filter(b => Math.abs(b.change) > 0.000001), // 过滤微小变化
      tokenBalanceChanges,
      tradeInfo
    };
    
    return res.json(result);
  } catch (error) {
    console.error('解析交易详情失败:', error);
    return res.status(500).json({
      success: false,
      error: error.message,
      hint: '请确保交易签名格式正确，且交易存在于链上'
    });
  }
}

export async function getAddressTransactionHistoryHandler(req, res) {
  try {
    const { address } = req.params;
    const limit = parseInt(req.query.limit) || 100;

    // 参数验证
    if (!address) {
      return res.status(400).json({
        success: false,
        error: '地址参数不能为空'
      });
    }

    // 检查 RPC 配置
    if (!checkRPCConfig()) {
      return res.status(400).json({
        success: false,
        error: '需要配置 dRPC 或 Helius API Key 才能使用此功能',
        hint: '请在 .env 文件中设置 DRPC_API_KEY（优先）或 HELIUS_API_KEY'
      });
    }

    // 限制查询数量（dRPC 通常有更高的限制，但为了安全起见仍设置上限）
    const maxLimit = config.drpc.apiKey ? 200 : 50;
    const safeLimit = Math.min(limit, maxLimit);
    
    if (limit > maxLimit) {
      const provider = config.drpc.apiKey ? 'dRPC' : 'Helius';
      console.warn(`查询数量 ${limit} 超过安全限制（${provider} 建议），已调整为 ${safeLimit}`);
    }

    // 调用 service 处理业务逻辑
    const transactions = await getAddressTransactionHistory(address, safeLimit);

    // 过滤交易类型（可选，通过 query 参数控制）
    // 例如: ?filterTypes=buy,sell 只返回买入和卖出交易
    const filterTypesParam = req.query.filterTypes;
    let filteredTransactions = transactions;
    
    if (filterTypesParam) {
      const filterTypes = filterTypesParam.split(',').map(t => t.trim().toLowerCase());
      filteredTransactions = transactions.filter(tx => 
        filterTypes.includes(tx.transactionType?.toLowerCase())
      );
    }

    // 解析每笔交易的买卖信息（可选，通过 query 参数控制）
    const parseBuySell = req.query.parseBuySell === 'true';
    let transactionsWithBuySell = filteredTransactions;

    if (parseBuySell && transactions.length > 0) {
      // 提取所有交易签名
      const signatures = transactions
        .map(tx => tx.signature)
        .filter(sig => sig); // 过滤掉空签名

      if (signatures.length > 0) {
        // 批量解析买卖信息（并发数 5，避免过载）
        const buySellInfoMap = await parseMultipleTradeInfo(signatures, 5);

        // 为每笔交易添加买卖信息
        transactionsWithBuySell = transactions.map(tx => {
          const buySellInfo = buySellInfoMap.get(tx.signature);
          return {
            ...tx,
            buySellInfo: buySellInfo || null
          };
        });
      }
    }

    // 返回结果
    return res.json({
      success: true,
      address,
      total: transactionsWithBuySell.length,
      limit,
      filterTypes: filterTypesParam || null,
      parseBuySell: parseBuySell,
      transactions: transactionsWithBuySell
    });
  } catch (error) {
    console.error('获取地址交易历史失败:', error);
    return res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

/**
 * 使用 Helius getTransactionsForAddress 获取地址交易历史（增强版）
 * 支持高级过滤、排序和分页
 */
export async function getTransactionsForAddressHandler(req, res) {
  try {
    const { address } = req.params;
    const limit = parseInt(req.query.limit) || 50;
    const transactionDetails = req.query.transactionDetails || 'signatures'; // 'signatures' 或 'full'
    const sortOrder = req.query.sortOrder || 'desc'; // 'asc' 或 'desc'
    const paginationToken = req.query.paginationToken || null;
    const parseFullTransactions = req.query.parseFullTransactions === 'true'; // 是否批量解析完整交易
    const shouldParseTradeInfo = req.query.parseTradeInfo === 'true'; // 是否解析交易类型（买卖/转移等）

    // 时区转换：默认将北京时间转换为 UTC
    // 如果用户明确指定传入的是 UTC 时间戳，可以通过 skipTimezoneConversion=true 跳过转换
    const skipTimezoneConversion = req.query.skipTimezoneConversion === 'true' || req.query.skipTimezoneConversion === '1';
    let blockTimeGte = req.query.blockTimeGte ? parseInt(req.query.blockTimeGte) : null;
    let blockTimeLt = req.query.blockTimeLt ? parseInt(req.query.blockTimeLt) : null;
    
    // 默认总是做时区转换（因为前端传入的通常都是北京时间）
    if (!skipTimezoneConversion && blockTimeGte && blockTimeLt) {
      const BEIJING_UTC_OFFSET = 8 * 60 * 60; // 8小时 = 28800秒
      // 转换公式：UTC时间戳 = 北京时间戳 - 28800秒
      blockTimeGte = blockTimeGte - BEIJING_UTC_OFFSET;
      blockTimeLt = blockTimeLt - BEIJING_UTC_OFFSET;
    }

    // 构建过滤条件
    const filters = {};
    if (req.query.status) {
      filters.status = req.query.status; // 'succeeded' 或 'failed'
    }
    if (req.query.slotGte) {
      filters.slot = filters.slot || {};
      filters.slot.gte = parseInt(req.query.slotGte);
    }
    if (req.query.slotLt) {
      filters.slot = filters.slot || {};
      filters.slot.lt = parseInt(req.query.slotLt);
    }
    if (blockTimeGte) {
      filters.blockTime = filters.blockTime || {};
      filters.blockTime.gte = blockTimeGte;
    }
    if (blockTimeLt) {
      filters.blockTime = filters.blockTime || {};
      // 使用 lte（小于等于）而不是 lt（小于），更符合 Helius API 文档
      filters.blockTime.lte = blockTimeLt;
    }

    // 参数验证
    if (!address) {
      return res.status(400).json({
        success: false,
        error: '地址参数不能为空'
      });
    }

    if (!config.helius.apiKey) {
      return res.status(400).json({
        success: false,
        error: '需要配置 Helius API Key 才能使用此功能',
        hint: '请在 .env 文件中设置 HELIUS_API_KEY'
      });
    }

    // 优化：如果 transactionDetails 是 'full'，Helius 已经返回了完整的交易数据
    // 不需要再设置 parseFullTransactions，避免额外的 RPC 调用
    const shouldParseFull = parseFullTransactions && transactionDetails !== 'full';
    
    // Helius API 限制：当 transactionDetails 为 'full' 时，最多只能请求 100 笔交易
    let effectiveLimit = limit;
    if (transactionDetails === 'full' && limit > 100) {
      effectiveLimit = 100;
      console.log(`⚠ 警告: transactionDetails='full' 时，Helius API 最多支持 100 笔交易，已将 limit 从 ${limit} 调整为 100`);
    }
    
    // 调用 service
    const result = await getTransactionsForAddress(address, {
      limit: effectiveLimit,
      transactionDetails, // 如果设置为 'full'，Helius 会直接返回完整交易数据
      sortOrder,
      filters: Object.keys(filters).length > 0 ? filters : undefined,
      paginationToken,
      parseFullTransactions: shouldParseFull // 只在 transactionDetails 不是 'full' 时才需要额外解析
    });

    // 如果需要解析交易类型（买卖/转移等）
    let tradeInfoMap = new Map();
    if (shouldParseTradeInfo && result.data && result.data.length > 0) {
      try {
        console.log(`开始批量解析 ${result.data.length} 笔交易的买卖信息...`);
        
        // 优化：如果 transactionDetails 是 'full'，数据已经包含 transaction 和 meta
        // 可以直接使用，无需额外的 RPC 调用
        const hasFullTransactions = result.data.some(tx => tx.transaction && tx.meta);
        
        if (hasFullTransactions && transactionDetails === 'full') {
          console.log(`✓ 使用 transactionDetails: 'full' 返回的完整交易数据（无需额外 RPC 调用）`);
          
          // 直接使用已有的交易数据解析
          const { parseMultipleTradeInfoFromTransactions } = await import('../services/transactionParseService.js');
          tradeInfoMap = await parseMultipleTradeInfoFromTransactions(result.data);
          console.log(`✓ 成功解析 ${tradeInfoMap.size} 笔交易的买卖信息（使用已有数据）`);
        } else {
          // 如果没有完整数据，使用签名批量获取
          const signatures = result.data
            .map(tx => tx.signature || tx.transaction?.signatures?.[0])
            .filter(sig => sig);
          
          if (signatures.length > 0) {
            // 对于少量交易，使用更大的批次一次性获取
            const concurrency = signatures.length <= 20 ? signatures.length : 50;
            console.log(`使用批量大小: ${concurrency}`);
            const { parseMultipleTradeInfo } = await import('../services/transactionParseService.js');
            tradeInfoMap = await parseMultipleTradeInfo(signatures, concurrency);
            console.log(`✓ 成功解析 ${tradeInfoMap.size} 笔交易的买卖信息`);
          }
        }
      } catch (error) {
        console.error('批量解析交易信息失败:', error.message);
        // 继续返回数据，只是没有交易类型信息
      }
    }

    // 收集所有代币地址（mint），用于批量获取元数据
    const tokenMints = new Set();
    tradeInfoMap.forEach(tradeInfo => {
      if (tradeInfo.soldToken?.mint) {
        tokenMints.add(tradeInfo.soldToken.mint);
      }
      if (tradeInfo.boughtToken?.mint) {
        tokenMints.add(tradeInfo.boughtToken.mint);
      }
    });

    // 批量获取代币元数据（符号）- 使用已导入的函数，避免动态导入延迟
    const tokenMetadataMap = new Map();
    if (tokenMints.size > 0) {
      try {
        console.log(`正在批量获取 ${tokenMints.size} 个代币的元数据（符号）...`);
        const tokenAddresses = Array.from(tokenMints);
        const metadataStartTime = Date.now();
        const metadataResults = await getTokenMetadataMultipleViaHelius(
          config.helius.apiKey,
          tokenAddresses
        );
        const metadataTime = Date.now() - metadataStartTime;
        
        metadataResults.forEach(metadata => {
          if (metadata && metadata.address) {
            tokenMetadataMap.set(metadata.address, metadata.symbol || 'Unknown');
          }
        });
        console.log(`✓ 成功获取 ${tokenMetadataMap.size} 个代币的符号，耗时 ${metadataTime}ms`);
      } catch (error) {
        console.warn(`批量获取代币元数据失败: ${error.message}，将使用默认符号`);
      }
    }

    // 将交易类型信息合并到交易数据中，并填充代币符号
    // 只保留页面展示需要的字段，去掉无用的交易详情
    const enrichedData = result.data.map(tx => {
      const signature = tx.signature || tx.transaction?.signatures?.[0];
      const tradeInfo = tradeInfoMap.get(signature);
      
      // 构建简化的交易数据，只保留展示需要的字段
      const simplifiedTx = {
        signature: signature,
        blockTime: tx.blockTime,
        slot: tx.slot,
        err: tx.err || tx.meta?.err || null,
        confirmationStatus: tx.confirmationStatus || null
      };

      // 如果有交易信息，添加并更新代币符号
      if (tradeInfo) {
        // 更新 soldToken 的符号
        let soldToken = { ...tradeInfo.soldToken };
        if (soldToken.mint && tokenMetadataMap.has(soldToken.mint)) {
          soldToken.symbol = tokenMetadataMap.get(soldToken.mint);
        } else if (!soldToken.symbol || soldToken.symbol === 'Unknown') {
          // 如果是 SOL，保持 SOL
          if (soldToken.mint === 'So11111111111111111111111111111111111111112') {
            soldToken.symbol = 'SOL';
          } else {
            soldToken.symbol = soldToken.symbol || 'Unknown';
          }
        }

        // 更新 boughtToken 的符号
        let boughtToken = { ...tradeInfo.boughtToken };
        if (boughtToken.mint && tokenMetadataMap.has(boughtToken.mint)) {
          boughtToken.symbol = tokenMetadataMap.get(boughtToken.mint);
        } else if (!boughtToken.symbol || boughtToken.symbol === 'Unknown') {
          // 如果是 SOL，保持 SOL
          if (boughtToken.mint === 'So11111111111111111111111111111111111111112') {
            boughtToken.symbol = 'SOL';
          } else {
            boughtToken.symbol = boughtToken.symbol || 'Unknown';
          }
        }

        simplifiedTx.tradeInfo = {
          type: tradeInfo.type, // 'buy', 'sell', 'swap', 'transfer'
          dex: tradeInfo.dex,
          source: tradeInfo.source,
          soldToken: soldToken,
          boughtToken: boughtToken,
          price: tradeInfo.price,
          fee: tradeInfo.fee,
          holderAddress: tradeInfo.holderAddress || null
        };
      }

      return simplifiedTx;
    });

    res.json({
      success: true,
      address,
      data: enrichedData,
      paginationToken: result.paginationToken,
      total: result.total,
      limit,
      transactionDetails,
      sortOrder,
      parseFullTransactions,
      parseTradeInfo: shouldParseTradeInfo,
      filters: Object.keys(filters).length > 0 ? filters : undefined,
      method: 'Helius getTransactionsForAddress (Enhanced)',
      provider: 'Helius'
    });
  } catch (error) {
    console.error('获取地址交易历史失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      hint: '请确保地址正确，或检查 Helius API Key 配置'
    });
  }
}

/**
 * 获取代币持仓占比汇总（按批次时间）
 */
export async function getTokenHoldersSummaryHandler(req, res) {
  try {
    const { tokenAddress } = req.params;

    if (!tokenAddress) {
      return res.status(400).json({
        success: false,
        error: '代币地址不能为空'
      });
    }

    const summary = await getTokenHoldersSummaryByBatchTime(tokenAddress);

    res.json({
      success: true,
      tokenAddress,
      summary: summary.map(item => ({
        batchTime: item.batch_time,
        addressCount: item.address_count,
        totalPercent: parseFloat(item.total_percent || 0),
        maxPercent: parseFloat(item.max_percent || 0),
        avgPercent: parseFloat(item.avg_percent || 0)
      }))
    });
  } catch (error) {
    console.error('获取代币持仓占比汇总失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

/**
 * 测试路由：批量获取代币元数据（使用 Birdeye API）
 * 文档: https://docs.birdeye.so/reference/get-defi-v3-token-meta-data-multiple
 * 最多支持 50 个代币地址
 */
/**
 * 测试路由：验证 Helius getTransactionsForAddress 的 transactionDetails: 'full' 返回的数据结构
 * 用于验证是否可以直接使用返回的数据，而不需要额外的 getParsedTransactions 调用
 */
export async function testHeliusTransactionDetailsHandler(req, res) {
  try {
    const { address } = req.params;
    const limit = parseInt(req.query.limit) || 5; // 默认只测试 5 条，减少数据量
    
    if (!address) {
      return res.status(400).json({
        success: false,
        error: '地址参数不能为空'
      });
    }

    if (!config.helius.apiKey) {
      return res.status(400).json({
        success: false,
        error: '需要配置 Helius API Key 才能使用此功能',
        hint: '请在 .env 文件中设置 HELIUS_API_KEY'
      });
    }

    console.log(`\n=== 开始测试 Helius getTransactionsForAddress transactionDetails 参数 ===`);
    console.log(`测试地址: ${address}`);
    console.log(`测试数量: ${limit}\n`);

    // 测试 1: transactionDetails: 'signatures'（仅签名）
    console.log('--- 测试 1: transactionDetails = "signatures" ---');
    const resultSignatures = await getTransactionsForAddress(address, {
      limit,
      transactionDetails: 'signatures',
      sortOrder: 'desc'
    });
    
    const sampleSignature = resultSignatures.data[0];
    console.log('返回数据结构（signatures 模式）:');
    console.log(JSON.stringify({
      signature: sampleSignature?.signature,
      hasTransaction: !!sampleSignature?.transaction,
      hasParsedTransaction: !!sampleSignature?.parsedTransaction,
      hasMeta: !!sampleSignature?.meta,
      keys: Object.keys(sampleSignature || {})
    }, null, 2));

    // 测试 2: transactionDetails: 'full'（完整详情）
    console.log('\n--- 测试 2: transactionDetails = "full" ---');
    const resultFull = await getTransactionsForAddress(address, {
      limit,
      transactionDetails: 'full',
      sortOrder: 'desc'
    });
    
    const sampleFull = resultFull.data[0];
    console.log('返回数据结构（full 模式）:');
    console.log(JSON.stringify({
      signature: sampleFull?.signature,
      hasTransaction: !!sampleFull?.transaction,
      hasParsedTransaction: !!sampleFull?.parsedTransaction,
      hasMeta: !!sampleFull?.meta,
      transactionType: sampleFull?.transaction ? typeof sampleFull.transaction : null,
      transactionKeys: sampleFull?.transaction ? Object.keys(sampleFull.transaction) : null,
      keys: Object.keys(sampleFull || {})
    }, null, 2));

    // 详细分析 full 模式的数据结构
    let fullAnalysis = null;
    if (sampleFull) {
      fullAnalysis = {
        signature: sampleFull.signature,
        slot: sampleFull.slot,
        blockTime: sampleFull.blockTime,
        hasTransaction: !!sampleFull.transaction,
        hasMeta: !!sampleFull.meta,
        transactionStructure: null,
        metaStructure: null
      };

      if (sampleFull.transaction) {
        fullAnalysis.transactionStructure = {
          type: typeof sampleFull.transaction,
          isArray: Array.isArray(sampleFull.transaction),
          keys: typeof sampleFull.transaction === 'object' ? Object.keys(sampleFull.transaction) : null,
          hasMessage: !!sampleFull.transaction.message,
          hasSignatures: !!sampleFull.transaction.signatures
        };
      }

      if (sampleFull.meta) {
        fullAnalysis.metaStructure = {
          keys: Object.keys(sampleFull.meta),
          hasErr: 'err' in sampleFull.meta,
          hasFee: 'fee' in sampleFull.meta,
          hasPreBalances: 'preBalances' in sampleFull.meta,
          hasPostBalances: 'postBalances' in sampleFull.meta,
          hasPreTokenBalances: 'preTokenBalances' in sampleFull.meta,
          hasPostTokenBalances: 'postTokenBalances' in sampleFull.meta,
          hasInnerInstructions: 'innerInstructions' in sampleFull.meta
        };
      }
    }

    // 测试 3: 对比是否需要额外的 getParsedTransactions 调用
    console.log('\n--- 测试 3: 检查 full 模式是否可以直接用于解析 ---');
    const { Connection } = await import('@solana/web3.js');
    const connection = new Connection(
      `https://mainnet.helius-rpc.com/?api-key=${config.helius.apiKey}`,
      'confirmed'
    );

    // 从 full 模式获取一个交易
    const testTransaction = resultFull.data[0];
    let comparisonResult = null;

    if (testTransaction && testTransaction.transaction) {
      const testSignature = testTransaction.transaction.signatures?.[0] || testTransaction.slot?.toString();
      
      // 使用标准的 getParsedTransaction 获取完整交易
      let parsedTx = null;
      if (testSignature && typeof testSignature === 'string' && testSignature.length > 20) {
        try {
          parsedTx = await connection.getParsedTransaction(testSignature, {
            commitment: 'confirmed',
            maxSupportedTransactionVersion: 0
          });
        } catch (error) {
          console.warn('获取标准 parsed transaction 失败:', error.message);
        }
      }

      comparisonResult = {
        fullModeHasTransaction: !!testTransaction.transaction,
        fullModeHasMeta: !!testTransaction.meta,
        fullModeTransactionKeys: testTransaction.transaction ? Object.keys(testTransaction.transaction) : null,
        fullModeMetaKeys: testTransaction.meta ? Object.keys(testTransaction.meta) : null,
        standardParsedTxType: parsedTx ? typeof parsedTx : null,
        standardParsedTxHasTransaction: parsedTx ? !!parsedTx.transaction : null,
        standardParsedTxHasMeta: parsedTx ? !!parsedTx.meta : null,
        canUseFullModeDirectly: false,
        recommendation: ''
      };

      // 检查 full 模式的数据结构是否完整
      const hasRequiredFields = 
        testTransaction.transaction?.message && 
        testTransaction.meta &&
        testTransaction.meta.preBalances &&
        testTransaction.meta.postBalances;

      if (hasRequiredFields) {
        // 如果 parsedTx 存在，进行详细对比
        if (parsedTx) {
          const fullTxKeys = Object.keys(testTransaction.transaction);
          const parsedTxKeys = parsedTx.transaction ? Object.keys(parsedTx.transaction) : [];
          
          comparisonResult.fullTxKeys = fullTxKeys;
          comparisonResult.parsedTxKeys = parsedTxKeys;
          comparisonResult.keysMatch = JSON.stringify(fullTxKeys.sort()) === JSON.stringify(parsedTxKeys.sort());
          
          // 检查 message 结构
          const fullHasMessage = !!testTransaction.transaction.message;
          const parsedHasMessage = !!parsedTx.transaction?.message;
          comparisonResult.messageStructureMatch = fullHasMessage && parsedHasMessage;
        }
        
        // 判断是否可以直接使用
        if (hasRequiredFields && testTransaction.meta.preTokenBalances !== undefined) {
          comparisonResult.canUseFullModeDirectly = true;
          comparisonResult.recommendation = '✅ 可以直接使用 full 模式返回的数据，无需额外调用 getParsedTransactions！这可以大幅提升性能。';
        } else {
          comparisonResult.recommendation = '⚠️ full 模式返回的数据可能缺少部分字段，建议对比后决定是否使用';
        }
      } else {
        comparisonResult.recommendation = '❌ full 模式未返回完整数据，需要额外调用 getParsedTransactions';
      }
    } else {
      comparisonResult = {
        error: '无法获取测试交易数据',
        recommendation: '需要检查 API 返回的数据结构'
      };
    }

    // 返回测试结果
    res.json({
      success: true,
      testAddress: address,
      testLimit: limit,
      results: {
        signaturesMode: {
          sample: {
            signature: sampleSignature?.signature,
            hasTransaction: !!sampleSignature?.transaction,
            hasParsedTransaction: !!sampleSignature?.parsedTransaction,
            hasMeta: !!sampleSignature?.meta,
            keys: Object.keys(sampleSignature || {})
          },
          totalTransactions: resultSignatures.data.length
        },
        fullMode: {
          sample: {
            signature: sampleFull?.signature,
            hasTransaction: !!sampleFull?.transaction,
            hasParsedTransaction: !!sampleFull?.parsedTransaction,
            hasMeta: !!sampleFull?.meta,
            keys: Object.keys(sampleFull || {})
          },
          detailedAnalysis: fullAnalysis,
          totalTransactions: resultFull.data.length
        },
        comparison: comparisonResult
      },
      conclusion: {
        canUseFullModeDirectly: comparisonResult?.canUseFullModeDirectly || false,
        recommendation: comparisonResult?.recommendation || '需要进一步测试',
        performanceImpact: comparisonResult?.canUseFullModeDirectly 
          ? '使用 full 模式可以避免额外的 getParsedTransactions 调用，大幅提升性能'
          : '需要额外的 getParsedTransactions 调用来获取完整交易详情'
      },
      note: '此测试用于验证 Helius getTransactionsForAddress 的 transactionDetails: "full" 参数是否返回完整的 parsed transaction 数据'
    });
  } catch (error) {
    console.error('测试 Helius transactionDetails 失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      hint: '请检查 Helius API Key 配置和网络连接'
    });
  }
}

export async function getTokenMetadataMultipleHandler(req, res) {
  try {
    // 从查询参数获取代币地址列表（支持逗号分隔或多个 address 参数）
    let tokenAddresses = [];
    
    if (req.query.addresses) {
      // 支持逗号分隔的地址列表
      tokenAddresses = req.query.addresses.split(',').map(addr => addr.trim()).filter(addr => addr);
    } else if (req.query.address) {
      // 支持单个地址
      tokenAddresses = [req.query.address];
    } else if (req.body && req.body.addresses && Array.isArray(req.body.addresses)) {
      // 支持 POST 请求的地址数组
      tokenAddresses = req.body.addresses;
    } else {
      return res.status(400).json({
        success: false,
        error: '缺少代币地址参数',
        hint: '请使用 ?addresses=ADDR1,ADDR2,ADDR3 或 ?address=ADDR1 或 POST body: { addresses: ["ADDR1", "ADDR2"] }',
        example: '/api/token/metadata/multiple?addresses=EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v,So11111111111111111111111111111111111111112'
      });
    }

    // 验证地址数量（最多 50 个）
    if (tokenAddresses.length === 0) {
      return res.status(400).json({
        success: false,
        error: '代币地址列表不能为空'
      });
    }

    // 限制批量数量（避免过多请求）
    const maxBatchSize = 50;
    if (tokenAddresses.length > maxBatchSize) {
      return res.status(400).json({
        success: false,
        error: '代币地址数量超过限制',
        hint: `最多支持 ${maxBatchSize} 个地址，当前: ${tokenAddresses.length}`
      });
    }

    if (!config.helius.apiKey) {
      return res.status(400).json({
        success: false,
        error: '未配置 Helius API Key',
        hint: '请在 .env 文件中设置 HELIUS_API_KEY',
        registerUrl: 'https://www.helius.dev/'
      });
    }

    console.log(`正在使用 Helius API 批量获取 ${tokenAddresses.length} 个代币的元数据...`);

    // 调用公共 service 方法
    const results = await getTokenMetadataMultipleViaHelius(
      config.helius.apiKey,
      tokenAddresses
    );

    const successCount = results.length;
    const failCount = tokenAddresses.length - successCount;

    console.log(`✓ 成功获取 ${successCount} 个代币元数据，失败 ${failCount} 个`);

    res.json({
      success: true,
      tokenAddresses,
      total: tokenAddresses.length,
      successCount,
      failCount,
      results: results, // 格式: [{address, symbol, supply, decimals}, ...]
      method: 'Helius DAS API - getAsset (Multiple)',
      provider: 'Helius'
    });
  } catch (error) {
    console.error('获取代币元数据失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      hint: '请检查网络连接和 API Key 配置'
    });
  }
}


/**
 * 从 Jupiter API 获取代币价格（通过 USDC 报价）
 * @param {string} tokenAddress - 代币地址
 * @param {number} tokenDecimals - 代币精度，默认 6
 * @returns {Promise<number|null>} 代币的 USD 价格
 */
async function getTokenPriceFromJupiter(tokenAddress, tokenDecimals = 6) {
  try {
    const USDC_MINT = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v'; // USDC
    // 使用 Jupiter Quote API 获取代币可以兑换多少 USDC
    // amount 使用 10^tokenDecimals，表示 1 个代币
    const amount = Math.pow(10, tokenDecimals);
    const apiUrl = `https://quote-api.jup.ag/v6/quote?inputMint=${tokenAddress}&outputMint=${USDC_MINT}&amount=${amount}&slippageBps=50`;
    
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'Accept': 'application/json',
      }
    });

    if (!response.ok) {
      return null;
    }

    const data = await response.json();
    // Jupiter 返回的 outAmount 是 outputMint (USDC) 的最小单位数量
    // USDC 有 6 位小数，所以需要除以 1e6
    if (data.outAmount) {
      const usdcAmount = parseFloat(data.outAmount) / 1e6; // USDC 有 6 位小数
      const tokenAmount = 1; // 我们查询的是 1 个代币
      const price = usdcAmount / tokenAmount;
      return price > 0 ? price : null;
    }
    return null;
  } catch (error) {
    console.warn(`从 Jupiter 获取代币 ${tokenAddress} 价格失败:`, error.message);
    return null;
  }
}

/**
 * 从 Birdeye API 批量获取代币 USD 价格（带备用方案）
 * @param {string[]} tokenAddresses - 代币地址数组
 * @returns {Promise<Array<{address: string, price: number}>>} 代币价格数组
 */
async function getTokenPricesFromBirdeye(tokenAddresses) {
  if (!config.birdeye?.apiKey || !tokenAddresses || tokenAddresses.length === 0) {
    return [];
  }

  const SOL_MINT = 'So11111111111111111111111111111111111111112';
  
  try {
    // 先获取 SOL 价格（如果列表中有 SOL）
    let solPrice = null;
    if (tokenAddresses.includes(SOL_MINT)) {
      solPrice = await getSolUsdPrice();
      if (solPrice) {
        console.log(`✓ 获取到 SOL 价格: $${solPrice}`);
      } else {
        console.warn('⚠️  无法获取 SOL 价格');
      }
    }

    // Birdeye API: GET /defi/token_overview?address={address}
    // 需要逐个请求，或者使用批量接口
    const results = [];
    const batchSize = 10; // 控制并发数
    
    for (let i = 0; i < tokenAddresses.length; i += batchSize) {
      const batch = tokenAddresses.slice(i, i + batchSize);
      const batchPromises = batch.map(async (address) => {
        // 如果是 SOL，直接使用已获取的价格
        if (address === SOL_MINT) {
          return { address, price: solPrice };
        }

        let price = null;
        
        // 1. 优先从 Birdeye API 获取
        try {
          const apiUrl = `https://public-api.birdeye.so/defi/token_overview?address=${address}`;
          const response = await fetch(apiUrl, {
            method: 'GET',
            headers: {
              'X-API-KEY': config.birdeye.apiKey,
              'accept': 'application/json'
            }
          });

          if (response.ok) {
            const data = await response.json();
            // Birdeye 返回的价格在 data.price 或 data.data.price
            price = data?.data?.price || data?.price || null;
            if (price && price > 0) {
              console.log(`✓ 从 Birdeye 获取到 ${address} 价格: $${price}`);
              return { address, price };
            }
          }
        } catch (error) {
          console.warn(`从 Birdeye 获取代币 ${address} 价格失败:`, error.message);
        }

        // 2. 如果 Birdeye 失败，尝试从 Jupiter API 获取（备用方案）
        if (!price || price <= 0) {
          console.log(`尝试从 Jupiter 获取代币 ${address} 价格...`);
          try {
            // 尝试不同的 decimals（6, 9, 18 是常见的）
            for (const decimals of [6, 9, 18]) {
              price = await getTokenPriceFromJupiter(address, decimals);
              if (price && price > 0) {
                console.log(`✓ 从 Jupiter 获取到 ${address} 价格: $${price} (decimals: ${decimals})`);
                return { address, price };
              }
            }
          } catch (error) {
            console.warn(`从 Jupiter 获取代币 ${address} 价格失败:`, error.message);
          }
        }

        return { address, price: null };
      });

      const batchResults = await Promise.all(batchPromises);
      results.push(...batchResults);
      
      // 添加延迟，避免请求过快
      if (i + batchSize < tokenAddresses.length) {
        await new Promise(resolve => setTimeout(resolve, 200));
      }
    }

    return results;
  } catch (error) {
    console.error('批量获取代币价格失败:', error);
    return tokenAddresses.map(address => ({ address, price: null }));
  }
}

/**
 * 从 Helius 交易数据中解析 swap 信息
 * @param {Object} transaction - Helius API 返回的交易对象
 * @param {string} walletAddress - 钱包地址（用于确定交易方向）
 * @returns {Promise<Object|null>} swap 信息，包含 tokenA, tokenB, 价格等
 */
async function parseSwapFromHeliusTransaction(transaction, walletAddress) {
  try {
    if (!transaction || transaction.type !== 'SWAP') {
      return null;
    }

    const SOL_MINT = 'So11111111111111111111111111111111111111112';
    // 稳定币 mint 地址（1:1 兑换 USD）
    const STABLE_COINS = {
      USDC: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
      USDT: 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
      USDC_LEGACY: 'A9mUU4qviSctJVPJdBJWkb28deg915LYJKrzQ19ji3FM', // USDC (legacy)
    };
    const STABLE_COIN_MINTS = Object.values(STABLE_COINS);
    const tokenTransfers = transaction.tokenTransfers || [];
    const nativeTransfers = transaction.nativeTransfers || [];
    const accountData = transaction.accountData || [];

    // 1. 从 accountData 中提取用户账户的余额变化（更准确）
    // 需要根据 walletAddress 过滤，只考虑该用户的余额变化
    const tokenBalanceChanges = [];
    let userSolChange = 0; // 用户的 SOL 余额变化
    
    for (const account of accountData) {
      // 检查是否是用户账户的 SOL 余额变化
      if (account.account === walletAddress && account.nativeBalanceChange !== undefined) {
        userSolChange = account.nativeBalanceChange / 1e9; // 转换为 SOL
      }
      
      // 提取用户账户的代币余额变化
      if (account.tokenBalanceChanges && account.tokenBalanceChanges.length > 0) {
        for (const change of account.tokenBalanceChanges) {
          // 只考虑属于该用户的代币账户变化
          if (change.userAccount === walletAddress) {
            const rawAmount = BigInt(change.rawTokenAmount?.tokenAmount || '0');
            const decimals = change.rawTokenAmount?.decimals || 0;
            const amount = Number(rawAmount) / Math.pow(10, decimals);
            
            tokenBalanceChanges.push({
              mint: change.mint,
              userAccount: change.userAccount,
              amount: amount, // 正数=增加，负数=减少
              rawAmount: rawAmount.toString(),
              decimals: decimals
            });
          }
        }
      }
    }

    // 2. 如果没有 accountData，从 tokenTransfers 中提取
    if (tokenBalanceChanges.length === 0) {
      const mintMap = new Map();
      for (const transfer of tokenTransfers) {
        const mint = transfer.mint;
        if (!mintMap.has(mint)) {
          mintMap.set(mint, {
            mint: mint,
            amount: 0,
            decimals: 9 // 默认 decimals，实际应该从其他地方获取
          });
        }
        const amount = parseFloat(transfer.tokenAmount) || 0;
        // 根据 fromUserAccount 和 toUserAccount 判断方向
        if (transfer.fromUserAccount === walletAddress) {
          mintMap.get(mint).amount -= amount;
        } else if (transfer.toUserAccount === walletAddress) {
          mintMap.get(mint).amount += amount;
        }
      }
      mintMap.forEach((value, mint) => {
        if (Math.abs(value.amount) > 0.00000001) {
          tokenBalanceChanges.push(value);
        }
      });
    }

    // 3. 识别 fromToken 和 toToken
    // fromToken: 减少的代币（卖出的代币）
    // toToken: 增加的代币（买入的代币）
    const soldToken = tokenBalanceChanges.find(change => change.amount < 0);
    const boughtToken = tokenBalanceChanges.find(change => change.amount > 0);

    // 如果 accountData 中没有 SOL 变化，尝试从 nativeTransfers 中识别 SOL 转移
    if (Math.abs(userSolChange) < 0.0001) {
      for (const transfer of nativeTransfers) {
        const amount = transfer.amount / 1e9; // 转换为 SOL
        if (transfer.fromUserAccount === walletAddress) {
          userSolChange -= amount;
        } else if (transfer.toUserAccount === walletAddress) {
          userSolChange += amount;
        }
      }
    }

    // 4. 构建 swap 信息
    let fromToken = null; // 卖出的代币
    let toToken = null;   // 买入的代币
    let price = null;
    let swapType = 'swap';

    // 统一处理：根据 soldToken、boughtToken 和 userSolChange 确定 fromToken、toToken
    if (soldToken && boughtToken) {
      // 标准 swap：两个代币都有变化
      fromToken = {
        mint: soldToken.mint,
        symbol: soldToken.mint === SOL_MINT ? 'SOL' : 'Unknown',
        amount: Math.abs(soldToken.amount),
        decimals: soldToken.decimals || 9
      };
      toToken = {
        mint: boughtToken.mint,
        symbol: boughtToken.mint === SOL_MINT ? 'SOL' : 'Unknown',
        amount: boughtToken.amount,
        decimals: boughtToken.decimals || 9
      };
    } else if (soldToken && Math.abs(userSolChange) > 0.0001 && userSolChange > 0) {
      // 卖出代币换 SOL（用户 SOL 增加，代币减少）
      fromToken = {
        mint: soldToken.mint,
        symbol: soldToken.mint === SOL_MINT ? 'SOL' : 'Unknown',
        amount: Math.abs(soldToken.amount),
        decimals: soldToken.decimals || 9
      };
      toToken = {
        mint: SOL_MINT,
        symbol: 'SOL',
        amount: userSolChange,
        decimals: 9
      };
    } else if (boughtToken && Math.abs(userSolChange) > 0.0001 && userSolChange < 0) {
      // 用 SOL 买入代币（用户 SOL 减少，代币增加）
      fromToken = {
        mint: SOL_MINT,
        symbol: 'SOL',
        amount: Math.abs(userSolChange),
        decimals: 9
      };
      toToken = {
        mint: boughtToken.mint,
        symbol: boughtToken.mint === SOL_MINT ? 'SOL' : 'Unknown',
        amount: boughtToken.amount,
        decimals: boughtToken.decimals || 9
      };
    }

    // 确定 swap 类型
    if (fromToken && toToken) {
      if (fromToken.mint === SOL_MINT) {
        swapType = 'buy'; // SOL -> Token = 买入
      } else if (toToken.mint === SOL_MINT) {
        swapType = 'sell'; // Token -> SOL = 卖出
      }

      // 计算 USD 价格
      if (fromToken.amount > 0) {
        if (toToken.mint === SOL_MINT) {
          // toToken 是 SOL，需要获取 SOL 价格
          const solPrice = await getSolUsdPrice();
          if (solPrice && solPrice > 0) {
            const toTokenUsdValue = toToken.amount * solPrice;
            price = toTokenUsdValue / fromToken.amount;
          }
        } else if (STABLE_COIN_MINTS.includes(toToken.mint)) {
          // toToken 是稳定币（USDC/USDT），本身就是 USD 计价，直接计算
          price = toToken.amount / fromToken.amount;
        } else if (STABLE_COIN_MINTS.includes(fromToken.mint)) {
          // fromToken 是稳定币，toToken 是代币
          // fromToken.amount 就是 USD 价值（稳定币 1:1 USD）
          // price = fromToken.amount / toToken.amount（每个 toToken 值多少 USD）
          price = fromToken.amount / toToken.amount;
        } else if (fromToken.mint === SOL_MINT) {
          // fromToken 是 SOL，toToken 是代币
          // 获取 SOL 价格，计算 fromToken 的 USD 价值
          const solPrice = await getSolUsdPrice();
          if (solPrice && solPrice > 0) {
            const fromTokenUsdValue = fromToken.amount * solPrice;
            price = fromTokenUsdValue / toToken.amount;
          } else {
            price = null;
          }
        } else {
          // 两个都不是 SOL 或稳定币，需要获取两个代币的价格来计算
          try {
            const tokenAddresses = [fromToken.mint, toToken.mint];
            const priceResults = await getTokenPricesFromBirdeye(tokenAddresses);
            const priceMap = new Map(priceResults.map(r => [r.address, r.price]));
            
            const fromTokenPrice = priceMap.get(fromToken.mint);
            const toTokenPrice = priceMap.get(toToken.mint);
            
            if (fromTokenPrice && fromTokenPrice > 0 && toTokenPrice && toTokenPrice > 0) {
              // 计算 USD 价格：price = (toToken.amount * toToken.price) / fromToken.amount
              const toTokenUsdValue = toToken.amount * toTokenPrice;
              price = toTokenUsdValue / fromToken.amount;
            } else {
              // 如果无法获取价格，设为 null
              price = null;
            }
          } catch (error) {
            console.warn(`获取代币价格失败:`, error.message);
            price = null;
          }
        }
      }
    }

    if (!fromToken || !toToken) {
      return null;
    }

    return {
      type: swapType,
      dex: transaction.source || 'UNKNOWN',
      fromToken: fromToken,  // 卖出的代币
      toToken: toToken,      // 买入的代币
      price: price,
      fee: transaction.fee ? transaction.fee / 1e9 : null,
      signature: transaction.signature,
      timestamp: transaction.timestamp,
      slot: transaction.slot
    };
  } catch (error) {
    console.error('解析 Helius swap 信息失败:', error);
    return null;
  }
}

// todo 这个功能等几天看看，接口是否正常
export async function getWalletPnlDetailsByHeliusTestHandler(req, res) {
    // 1. 从路径参数获取钱包地址
    const {walletAddress} = req.params;

    if (!walletAddress || walletAddress.trim() === '') {
      return res.status(400).json({
        success: false,
        error: '钱包地址不能为空'
      });
    }

    // 2. 验证API密钥配置
    if (!config?.helius?.apiKey) {
      console.error('❌ 配置错误: 未找到Helius API密钥');
      return res.status(500).json({
        success: false,
        error: '服务器配置错误',
        hint: '请在config中配置helius.apiKey或在环境变量中设置HELIUS_API_KEY'
      });
    }

    const apiKey = config.helius.apiKey;

    // 3. 定义 SOL 和稳定币 mint 地址
    const SOL_MINT = 'So11111111111111111111111111111111111111112';
    const STABLE_COINS = {
      USDC: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', // USDC
      USDT: 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB', // USDT
      USDC_LEGACY: 'A9mUU4qviSctJVPJdBJWkb28deg915LYJKrzQ19ji3FM', // USDC (legacy)
    };
    const STABLE_COIN_MINTS = Object.values(STABLE_COINS);

    // 4. 自动循环查询最多1000条 SWAP 交易
    const MAX_TRANSACTIONS = 1000; // 最大交易笔数限制
    let allTransactions = [];

    let transactionType = "SWAP";
    let before = null;
    let pageCount = 0;
    const limit = 100; // 每页查询100笔

    let transferCount = 0;

    while (true) {
      pageCount++;

      console.log(`📊 开始查询第:${pageCount}页， ${before} 之前的100条数据...`);

      // todo 这个地方要是能用 SWAP 过滤会省下很多 api 调用
      const urlParams = new URLSearchParams();
      urlParams.append('type', transactionType);
      urlParams.append('limit', limit.toString());
      if (before != null) {
        urlParams.append("before", before)
      }

      const queryString = urlParams.toString();
      const apiUrl = `https://api.helius.xyz/v0/addresses/${walletAddress}/transactions?api-key=${apiKey}&${queryString}`;

      // 发送请求
      const response = await fetch(apiUrl, {
        method: 'GET',
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`❌ Helius API 错误:`, {status: response.status, error: errorText});

        // 这里只处理404异常码的
        if (response.status === 404) {
          if (transferCount < 2) {
            // 这个地方单独设置一下，因为有点地址有很多 tansfer, 需要转换为tranfer查询一下看看多两页后有没有swap
            transactionType = "TRANSFER";
            transferCount += 1;
            console.log(`查询swap为空,第: ${transferCount} 次`)
            continue;
          }else {
            console.error(`地址: ${walletAddress} transfer信息过多，看看是否单独验证地址盈利详情`);
            break;
          }
        }
      }

      let transactions = await response.json();

      // 检查是否返回了分页令牌
      if (transactions != null && transactions.length > 0) {
        if (transactionType === "swap") {
          // 这个地方只存swap类型的数据
          allTransactions.push(...transactions);
        }

        transactions.sort((a, b) => b.timestamp - a.timestamp);
        before = transactions[transactions.length - 1].signature;
        transactionType = "SWAP";
      }


      const currentPageCount = transactions.length || transactions.transactions?.length || 0;
      console.log(`✓ 第 ${pageCount} 页获取到 ${currentPageCount} 笔交易，累计: ${allTransactions.length} 笔`);

      // 如果达到最大交易数限制，停止查询
      if (allTransactions.length >= MAX_TRANSACTIONS) {
        console.log(`⚠️  交易笔数已达到最大限制 ${MAX_TRANSACTIONS} 笔，停止查询`);
        break;
      }

      if (transactions.length === 0) {
        break;
      }

      if (before == null) {
        console.log(`⚠ before 为null,停止查询`);
        break;
      }

      // 添加延迟，避免请求过快
      // await new Promise(resolve => setTimeout(resolve, 100));
    }
}

export async function getWalletPnlDetailsHandler(req, res) {
  try {
    const { walletAddress } = req.params;
    
    if (!walletAddress) {
      return res.status(400).json({
        success: false,
        error: '钱包地址不能为空'
      });
    }

    if (!config.birdeye.apiKey) {
      return res.status(400).json({
        success: false,
        error: '未配置 Birdeye API Key',
        hint: '请在 .env 文件中设置 BIRDEYE_API_KEY',
        registerUrl: 'https://birdeye.so/'
      });
    }

    // 检查是否启用自动分页（默认启用）
    const autoPaginate = req.query.autoPaginate !== 'false';
    const maxLimit = parseInt(req.query.maxLimit) || 100; // 每次请求最大限制
    const limit = Math.min(parseInt(req.query.limit) || maxLimit, maxLimit);

    // 如果用户明确指定了 offset，且禁用了自动分页，则只请求一次
    if (!autoPaginate && req.query.offset !== undefined) {
      return await getSinglePagePnlDetails(walletAddress, req, res);
    }

    // 获取过滤相关参数
    const enableFilter = req.query.enableFilter === 'true' || req.query.enableFilter === '1';
    const minPnlUsd = parseInt(req.query.minPnlUsd) || 5000;

    // 自动分页：循环获取所有代币
    console.log(`正在获取钱包 ${walletAddress} 的所有代币盈亏详情（自动分页）...`);
    if (enableFilter) {
      console.log(`启用过滤条件（最小盈利阈值: ${minPnlUsd} USD）`);
    }
    
    const allTokens = [];
    const allTokenAddresses = new Set(); // 用于检测重复
    let offset = 0;
    let pageCount = 0;
    const maxPages = parseInt(req.query.maxPages) || 100; // 最多分页次数，防止无限循环
    let hasMore = true;
    let shouldStop = false; // 是否因过滤条件停止
    const retryDelay = 2000; // 429 错误重试延迟（毫秒）
    const maxRetries = 3; // 最大重试次数

    while (hasMore && pageCount < maxPages && !shouldStop) {
      pageCount++;

      const requestBody = {
        wallet: walletAddress,
        sort_type: req.query.sort_type || 'desc',
        sort_by: req.query.sort_by || 'last_trade',
        limit: limit,
        offset: offset
      };

      // 如果有其他查询参数，也加上
      if (req.query.token) {
        requestBody.token = req.query.token;
      }

      const apiUrl = `https://public-api.birdeye.so/wallet/v2/pnl/details`;
      
      // 重试机制
      let retryCount = 0;
      let response = null;
      let data = null;
      
      while (retryCount <= maxRetries) {
        try {
          response = await fetch(apiUrl, {
            method: 'POST',
            headers: {
              'X-API-KEY': config.birdeye.apiKey,
              'accept': 'application/json',
              'x-chain': 'solana',
              'content-type': 'application/json'
            },
            body: JSON.stringify(requestBody)
          });

          if (response.status === 429) {
            // 429 Too Many Requests，需要等待后重试
            retryCount++;
            if (retryCount <= maxRetries) {
              const waitTime = retryDelay * retryCount; // 递增等待时间
              console.warn(`⚠️  请求过于频繁 (429)，等待 ${waitTime}ms 后重试 (${retryCount}/${maxRetries})...`);
              await new Promise(resolve => setTimeout(resolve, waitTime));
              continue;
            } else {
              const errorText = await response.text();
              console.error(`❌ Birdeye API 错误: ${response.status} - ${errorText}`);
              return res.status(response.status).json({
                success: false,
                error: `Birdeye API 返回错误: ${response.status} - Too many requests`,
                details: errorText,
                suggestion: '请稍后重试，或减少请求频率'
              });
            }
          }

          if (!response.ok) {
            const errorText = await response.text();
            console.error(`❌ Birdeye API 错误: ${response.status} - ${errorText}`);
            return res.status(response.status).json({
              success: false,
              error: `Birdeye API 返回错误: ${response.status}`,
              details: errorText
            });
          }

          data = await response.json();
          break; // 成功，退出重试循环
        } catch (fetchError) {
          retryCount++;
          if (retryCount <= maxRetries) {
            console.warn(`⚠️  请求异常，等待 ${retryDelay}ms 后重试 (${retryCount}/${maxRetries}):`, fetchError.message);
            await new Promise(resolve => setTimeout(resolve, retryDelay));
            continue;
          } else {
            throw fetchError;
          }
        }
      }

      if (data.success === false) {
        console.error(`❌ Birdeye API 返回失败:`, data);
        return res.status(400).json({
          success: false,
          error: 'Birdeye API 返回失败',
          details: data
        });
      }

      const tokens = data?.data?.tokens || [];

      if (tokens.length === 0) {
        // 没有更多数据
        hasMore = false;
        console.log(`[分页 ${pageCount}] 没有更多数据，停止分页`);
        break;
      }

      // 如果是第一次查询且启用了过滤，检查前100条是否满足条件
      if (enableFilter && pageCount === 1) {
        const meetsCondition = checkTokensFilterCondition(tokens, minPnlUsd);
        if (!meetsCondition) {
          console.log(`钱包 ${walletAddress} 的前100条代币不满足过滤条件（最小盈利阈值: ${minPnlUsd} USD），停止查询`);
          shouldStop = true;
          // 返回空数据
          return res.json({
            success: true,
            walletAddress,
            message: `前100条代币不满足过滤条件（最小盈利阈值: ${minPnlUsd} USD）`,
            pagination: {
              totalTokens: 0,
              pages: 1,
              limit: limit,
              autoPaginated: true,
              stoppedByFilter: true
            },
            data: {
              tokens: [],
              token_metadata: {}
            }
          });
        }
        console.log(`钱包 ${walletAddress} 的前100条代币满足过滤条件（最小盈利阈值: ${minPnlUsd} USD），继续查询`);
      }

      // 打印详细的代币信息用于对比
      const tokenAddresses = tokens.map(t => t.address || t.mint || 'N/A');
      
      // 检查是否有重复（如果 offset 不生效，可能会返回相同的数据）
      if (pageCount > 1 && tokens.length > 0) {
        const firstTokenAddress = tokenAddresses[0];
        const lastTokenAddress = allTokens[allTokens.length - 1]?.address || allTokens[allTokens.length - 1]?.mint;
        
        if (firstTokenAddress === lastTokenAddress) {
          hasMore = false;
          break;
        }
      }

      // 检查本页内是否有重复
      const duplicateInPage = tokenAddresses.filter((addr, index) => tokenAddresses.indexOf(addr) !== index);
      if (duplicateInPage.length > 0) {
        console.warn(`⚠️  本页内发现重复代币地址: ${duplicateInPage.join(', ')}`);
      }

      // 检查与已获取的代币是否有重复
      const duplicateWithPrevious = tokenAddresses.filter(addr => allTokenAddresses.has(addr));
      if (duplicateWithPrevious.length > 0) {
        console.warn(`⚠️  与已获取的代币有重复 (${duplicateWithPrevious.length} 个): ${duplicateWithPrevious.slice(0, 5).join(', ')}${duplicateWithPrevious.length > 5 ? '...' : ''}`);
      }

      // 添加到总列表
      allTokens.push(...tokens);
      tokenAddresses.forEach(addr => allTokenAddresses.add(addr));

      // 如果返回的数量小于 limit，说明已经获取完所有数据
      if (tokens.length < limit) {
        hasMore = false;
        console.log(`[分页 ${pageCount}] 已获取所有数据（返回数量 ${tokens.length} < limit ${limit}）`);
        break;
      }

      // 更新 offset 继续获取下一页
      offset += limit;
      
      // 添加延迟，避免请求过快（429 错误）
      if (hasMore && pageCount < maxPages && !shouldStop) {
        const delay = 1000; // 增加到 1 秒，避免 429 错误

        await new Promise(resolve => setTimeout(resolve, delay));
      }
    }

    console.log(`\n✅ 分页完成: 共获取 ${allTokens.length} 个代币，分 ${pageCount} 页`);

    // 构建响应数据
    const responseData = {
      success: true,
      data: {
        tokens: allTokens,
        token_metadata: {} // 如果有元数据，可以从最后一次请求中获取
      }
    };

    // 保存数据到数据库（只有未被过滤停止时才保存）
    if (responseData.success && !shouldStop && allTokens.length > 0) {
      try {
        const { saveWalletPnlToDatabase } = await import('../services/pnlService.js');
        const saveResult = await saveWalletPnlToDatabase(walletAddress, responseData);
        console.log(`保存盈亏数据结果:`, saveResult);
      } catch (saveError) {
        // 保存失败不影响 API 响应，只记录错误日志
        console.error('保存盈亏数据到数据库失败（不影响 API 响应）:', saveError);
      }
    } else if (shouldStop) {
      console.log(`⚠️  钱包 ${walletAddress} 不满足过滤条件，未保存数据`);
    }

    res.json({
      success: true,
      walletAddress,
      pagination: {
        totalTokens: allTokens.length,
        pages: pageCount,
        limit: limit,
        autoPaginated: true,
        stoppedByFilter: shouldStop
      },
      data: responseData.data
    });
  } catch (error) {
    console.error('获取钱包盈亏详情失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

/**
 * 检查代币是否满足过滤条件
 * @param {Array} tokens - 代币列表
 * @param {number} minPnlUsd - 最小盈利金额阈值（USD），默认 10000
 * @returns {boolean} 是否满足条件
 */
function checkTokensFilterCondition(tokens, minPnlUsd = 10000) {
  if (!tokens || tokens.length === 0) {
    return false;
  }

  // 检查前100条（或更少）
  const tokensToCheck = tokens.slice(0, 100);
  let sumPnlUsd = 0; // 累计盈利金额

  for (const token of tokensToCheck) {
    const pricing = token.pricing || {};
    const cashflow = token.cashflow_usd || {};
    
    let meetsCondition = false;
    let tokenPnlUsd = 0;
    
    // 条件1: avg_sell_cost / avg_buy_cost > 2
    const avgBuyCost = pricing.avg_buy_cost || 0;
    const avgSellCost = pricing.avg_sell_cost || 0;
    if (avgBuyCost > 0 && avgSellCost / avgBuyCost > 2) {
      meetsCondition = true;
    }
    
    // 条件2: total_invested / (cost_of_quantity_sold + current_value) > 2
    const totalInvested = cashflow.total_invested || 0;
    const costOfQuantitySold = cashflow.cost_of_quantity_sold || 0;
    const currentValue = cashflow.current_value || 0;
    const denominator = costOfQuantitySold + currentValue;
    
    if (!meetsCondition && denominator > 0 && totalInvested / denominator > 2) {
      meetsCondition = true;
    }
    
    // 如果满足任一条件，计算该代币的盈利金额
    if (meetsCondition) {
      // 盈利金额 = denominator - totalInvested
      tokenPnlUsd = denominator - totalInvested;
      sumPnlUsd += tokenPnlUsd;
    }
  }
  
  // 只有当累计盈利金额 > minPnlUsd 时才返回 true
  return sumPnlUsd > minPnlUsd;
}

/**
 * 生成钱包 PnL 详情的缓存键
 * @param {string} walletAddress - 钱包地址
 * @param {Object} options - 选项参数
 * @returns {string} 缓存键
 */
function getWalletPnlCacheKey(walletAddress, options = {}) {
  // 将关键参数包含在缓存键中，确保不同参数使用不同的缓存
  const { limit = 100, maxPages = 100, enableFilter = false, minPnlUsd = 10000 } = options;
  const paramsHash = `${limit}_${maxPages}_${enableFilter}_${minPnlUsd}`;
  return `wallet:pnl:${walletAddress}:${paramsHash}`;
}

/**
 * 获取钱包 PnL 详情（可复用函数，支持过滤条件和缓存）
 * @param {string} walletAddress - 钱包地址
 * @param {Object} options - 选项
 * @param {number} options.limit - 每页限制
 * @param {number} options.maxPages - 最大页数
 * @param {boolean} options.enableFilter - 是否启用过滤（第一次查询前100条不满足条件则停止）
 * @param {boolean} options.saveToDatabase - 是否保存到数据库
 * @param {number} options.minPnlUsd - 最小盈利金额阈值（USD），默认 10000
 * @param {number} options.cacheTtl - 缓存过期时间（秒），默认 3600（1小时）
 * @param {boolean} options.useCache - 是否使用缓存，默认 true
 * @returns {Promise<Object>} 返回数据
 */
async function getWalletPnlDetailsInternal(walletAddress, options = {}) {
  const {
    limit = 100,
    maxPages = 100,
    enableFilter = false,
    saveToDatabase = true,
    minPnlUsd = 10000,
    cacheTtl = 3600, // 默认 1 小时
    useCache = true
  } = options;

  if (!config.birdeye.apiKey) {
    throw new Error('未配置 Birdeye API Key');
  }

  // 检查缓存
  if (useCache) {
    try {
      const CacheService = (await import('../services/cacheService.js')).default;
      const cacheKey = getWalletPnlCacheKey(walletAddress, options);
      const cached = await CacheService.get(cacheKey);
      
      if (cached !== null) {
        console.log(`✓ 从缓存获取钱包 ${walletAddress} 的 PnL 详情`);
        return cached;
      }
    } catch (cacheError) {
      console.warn(`从缓存获取钱包 ${walletAddress} 的 PnL 详情失败:`, cacheError.message);
      // 缓存失败不影响主流程，继续执行
    }
  }

  const allTokens = [];
  const allTokenAddresses = new Set();
  let offset = 0;
  let pageCount = 0;
  let hasMore = true;
  const retryDelay = 2000;
  const maxRetries = 3;
  let shouldStop = false;

  while (hasMore && pageCount < maxPages && !shouldStop) {
    pageCount++;

    const requestBody = {
      wallet: walletAddress,
      sort_type: 'desc',
      sort_by: 'last_trade',
      limit: limit,
      offset: offset
    };

    const apiUrl = `https://public-api.birdeye.so/wallet/v2/pnl/details`;
    
    let retryCount = 0;
    let response = null;
    let data = null;
    
    while (retryCount <= maxRetries) {
      try {
        response = await fetch(apiUrl, {
          method: 'POST',
          headers: {
            'X-API-KEY': config.birdeye.apiKey,
            'accept': 'application/json',
            'x-chain': 'solana',
            'content-type': 'application/json'
          },
          body: JSON.stringify(requestBody)
        });

        if (response.status === 429) {
          retryCount++;
          if (retryCount <= maxRetries) {
            const waitTime = retryDelay * retryCount;
            console.warn(`⚠️  请求过于频繁 (429)，等待 ${waitTime}ms 后重试 (${retryCount}/${maxRetries})...`);
            await new Promise(resolve => setTimeout(resolve, waitTime));
            continue;
          } else {
            await response.text();
            throw new Error(`Birdeye API 返回错误: ${response.status} - Too many requests`);
          }
        }

        if (!response.ok) {
          const errorText = await response.text();
          throw new Error(`Birdeye API 返回错误: ${response.status} - ${errorText}`);
        }

        data = await response.json();
        break;
      } catch (fetchError) {
        retryCount++;
        if (retryCount <= maxRetries) {
          console.warn(`⚠️  请求异常，等待 ${retryDelay}ms 后重试 (${retryCount}/${maxRetries}):`, fetchError.message);
          await new Promise(resolve => setTimeout(resolve, retryDelay));
          continue;
        } else {
          throw fetchError;
        }
      }
    }

    if (data.success === false) {
      throw new Error('Birdeye API 返回失败');
    }

    const tokens = data?.data?.tokens || [];

    if (tokens.length === 0) {
      hasMore = false;
      break;
    }

    // 如果是第一次查询且启用了过滤，检查前100条是否满足条件
    if (enableFilter && pageCount === 1) {
      // 找出聪明钱包，过滤机器人地址
      const meetsCondition = checkTokensFilterCondition(tokens, minPnlUsd);
      if (!meetsCondition) {
        console.log(`钱包 ${walletAddress} 的前100条代币不满足过滤条件（最小盈利阈值: ${minPnlUsd} USD），停止查询该地址`);
        shouldStop = true;
        // 不保存已获取的数据，直接返回
        return {
          success: true,
          data: {
            tokens: [],  // 不返回任何代币数据
            token_metadata: {}
          },
          walletAddress,
          pagination: {
            totalTokens: 0,
            pages: 1,
            limit: limit,
            stoppedByFilter: true
          }
        };
      }
      console.log(`钱包 ${walletAddress} 的前100条代币满足过滤条件（最小盈利阈值: ${minPnlUsd} USD），继续查询`);
    }

    // 添加到总列表
    allTokens.push(...tokens);
    tokens.forEach(t => {
      const addr = t.address || t.mint;
      if (addr) allTokenAddresses.add(addr);
    });

    // 如果返回的数量小于 limit，说明已经获取完所有数据
    if (tokens.length < limit) {
      hasMore = false;
      break;
    }

    // 更新 offset 继续获取下一页
    offset += limit;
    
    // 添加延迟，避免请求过快
    if (hasMore && pageCount < maxPages && !shouldStop) {
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }

  const responseData = {
    success: true,
    data: {
      tokens: allTokens,
      token_metadata: {}
    }
  };

  // 保存数据到数据库（只有满足条件且未被过滤停止时才保存）
  if (saveToDatabase && responseData.success && !shouldStop && allTokens.length > 0) {
    try {
      const { saveWalletPnlToDatabase } = await import('../services/pnlService.js');
      await saveWalletPnlToDatabase(walletAddress, responseData);
      console.log(`✓ 钱包 ${walletAddress} 的盈亏数据已保存到数据库（${allTokens.length} 个代币）`);
    } catch (saveError) {
      console.error(`保存钱包 ${walletAddress} 的盈亏数据失败:`, saveError);
    }
  } else if (shouldStop) {
    console.log(`⚠️  钱包 ${walletAddress} 不满足过滤条件，未保存数据`);
  }

  const result = {
    ...responseData,
    walletAddress,
    pagination: {
      totalTokens: allTokens.length,
      pages: pageCount,
      limit: limit,
      stoppedByFilter: shouldStop
    }
  };

  // 保存到缓存（只有成功获取数据且未被过滤停止时才缓存）
  if (useCache && !shouldStop && allTokens.length > 0) {
    try {
      const CacheService = (await import('../services/cacheService.js')).default;
      const cacheKey = getWalletPnlCacheKey(walletAddress, options);
      await CacheService.set(cacheKey, result, cacheTtl);
      console.log(`✓ 钱包 ${walletAddress} 的 PnL 详情已缓存（TTL: ${cacheTtl}秒）`);
    } catch (cacheError) {
      console.warn(`保存钱包 ${walletAddress} 的 PnL 详情到缓存失败:`, cacheError.message);
      // 缓存失败不影响主流程
    }
  }

  return result;
}

/**
 * 获取单页代币盈亏详情（不自动分页）
 */
async function getSinglePagePnlDetails(walletAddress, req, res) {
  const apiUrl = `https://public-api.birdeye.so/wallet/v2/pnl/details`;
  
  const offset = req.query.offset !== undefined ? parseInt(req.query.offset) : 0;
  const limit = parseInt(req.query.limit) || 30;
  
  const requestBody = {
    wallet: walletAddress,
    sort_type: req.query.sort_type || 'desc',
    sort_by: req.query.sort_by || 'last_trade',
    limit: limit,
    offset: offset
  };

  if (req.query.token) {
    requestBody.token = req.query.token;
  }

  console.log(`正在获取钱包 ${walletAddress} 的代币盈亏详情（单页）...`);
  console.log(`请求体:`, JSON.stringify(requestBody, null, 2));

  const response = await fetch(apiUrl, {
    method: 'POST',
    headers: {
      'X-API-KEY': config.birdeye.apiKey,
      'accept': 'application/json',
      'x-chain': 'solana',
      'content-type': 'application/json'
    },
    body: JSON.stringify(requestBody)
  });

  if (!response.ok) {
    const errorText = await response.text();
    console.error(`Birdeye API 错误: ${response.status} - ${errorText}`);
    return res.status(response.status).json({
      success: false,
      error: `Birdeye API 返回错误: ${response.status}`,
      details: errorText
    });
  }

  const data = await response.json();

  // 保存数据到数据库（异步执行，不影响 API 响应）
  if (data && data.success !== false) {
    try {
      const { saveWalletPnlToDatabase } = await import('../services/pnlService.js');
      const saveResult = await saveWalletPnlToDatabase(walletAddress, data);
      console.log(`保存盈亏数据结果:`, saveResult);
    } catch (saveError) {
      console.error('保存盈亏数据到数据库失败（不影响 API 响应）:', saveError);
    }
  }

  res.json({
    success: true,
    walletAddress,
    data: data
  });
}

/**
 * 获取代币持有人列表（使用 Helius getProgramAccountsV2，支持分页获取所有账户）
 * 文档: https://www.helius.dev/docs/rpc/guides/getprogramaccounts
 */
export async function getTokenHoldersV2Handler(req, res) {
  try {
    const { tokenAddress } = req.params;
    const filterPools = req.query.filterPools === 'true'; // 默认 false，需要显式启用
    const checkOnChain = req.query.checkOnChain === 'true'; // 默认 false，需要显式启用
    const enableClustering = req.query.enableClustering === 'true'; // 默认 false，地址聚类较慢
    
    if (!config.helius.apiKey) {
      return res.status(400).json({
        success: false,
        error: '未配置 Helius API Key',
        hint: '请在 .env 文件中设置 HELIUS_API_KEY',
        registerUrl: 'https://www.helius.dev/'
      });
    }

    console.log(`开始使用 Helius getProgramAccountsV2 获取代币 ${tokenAddress} 的所有持有人...`);

    // 使用 getProgramAccountsV2 方法获取所有持有人
    const { getTokenHoldersViaHeliusV2 } = await import('../src/tokenTracker.js');
    const holders = await getTokenHoldersViaHeliusV2(
      config.helius.apiKey,
      tokenAddress
    );

    if (!holders || holders.length === 0) {
      return res.json({
        success: true,
        tokenAddress,
        totalHolders: 0,
        message: '未找到持有人',
        holders: []
      });
    }

    // 应用过滤和聚类逻辑
    let filteredHolders = holders;
    let filteredCount = 0;

    if (filterPools || checkOnChain) {
      const { filterLiquidityPoolsAndExchanges } = await import('../services/tokenSyncService.js');
      const filterResult = await filterLiquidityPoolsAndExchanges(
        holders,
        filterPools,
        checkOnChain,
        console.log
      );
      filteredHolders = filterResult.filteredHolders;
      filteredCount = filterResult.filteredCount;
    }

    // 应用地址聚类（如果启用，在过滤流动池之后执行）
    if (enableClustering && filteredHolders.length > 0) {
      console.log(`启用地址聚类分析...`);
      try {
        const { mergeClusteredHolders } = await import('../services/addressClusteringService.js');
        filteredHolders = await mergeClusteredHolders(filteredHolders, true);
        console.log(`地址聚类完成，剩余 ${filteredHolders.length} 个聚类后的地址`);
      } catch (error) {
        console.warn(`地址聚类失败: ${error.message}，继续使用未聚类的数据`);
      }
    }

    res.json({
      success: true,
      tokenAddress,
      totalHolders: filteredHolders.length,
      filtered: filteredCount,
      method: 'Helius getProgramAccountsV2',
      holders: filteredHolders,
      clustered: enableClustering
    });
  } catch (error) {
    console.error('使用 Helius getProgramAccountsV2 获取代币持有人失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      hint: '请确保代币地址正确，或尝试使用标准方法: GET /api/token/:tokenAddress/holders',
      suggestion: '此方法使用 Helius getProgramAccountsV2，支持分页获取所有账户（最多 10,000 个）'
    });
  }
}

/**
 * 自动循环获取时间范围内的所有交易，并保存买入交易到数据库
 * 当买入金额 > 0.5 SOL 时记录到数据库，卖出不记录
 */
/**
 * 构建同步过滤条件
 */
function buildSyncFilters(blockTimeGte, blockTimeLt) {
  const filters = {
    status: 'succeeded' // 只获取成功的交易
  };

  // 使用 blockTime 过滤
  if (blockTimeGte && blockTimeLt) {
    // 注意：Helius API 可能不支持 lte，使用 lt 更安全
    filters.blockTime = {
      gte: blockTimeGte,
      lt: blockTimeLt + 1 // 加1秒以确保包含结束时间
    };
    console.log(`[后台任务] 使用 blockTime 过滤: [${blockTimeGte}, ${blockTimeLt}]`);
  }

  return filters;
}

/**
 * 处理单笔交易，判断是否应该保存
 */
function processSingleTransaction(
  tx,
  tradeInfo,
  address,
  tokenMetadataMap,
  robotAddresses,
  blockTimeGte,
  blockTimeLt,
  SOL_MINT,
  MIN_SOL_AMOUNT,
  stats
) {
  const signature = tx.signature || tx.transaction?.signatures?.[0];
  if (!signature) {
    return null;
  }

  // 客户端二次验证：检查交易是否真的在时间范围内（Helius API 可能返回范围外的交易）
  if (blockTimeGte && blockTimeLt && tx.blockTime) {
    if (tx.blockTime < blockTimeGte || tx.blockTime >= blockTimeLt) {
      stats.skippedOutOfTimeRange++;
      return null;
    }
  }

  if (!tradeInfo) {
    stats.skippedNoTradeInfo++;
    return null; // 无法解析的交易跳过
  }

  // 只处理买入交易（type === 'buy'）和交换交易（type === 'swap'）
  if (tradeInfo.type !== 'buy' && tradeInfo.type !== 'swap') {
    stats.skippedNotBuy++;
    return null; // 卖出不记录
  }

  const soldToken = tradeInfo.soldToken;
  const boughtToken = tradeInfo.boughtToken;

  if (!soldToken || !boughtToken) {
    stats.skippedNotSOLBuy++;
    return null;
  }

  // 对于 buy 类型：确保是 SOL 买入代币（soldToken 是 SOL，boughtToken 是代币）
  // 对于 swap 类型：确保是代币到代币交换（soldToken 和 boughtToken 都不是 SOL）
  let isSOLBuyToken = false;
  let isTokenSwap = false;
  let solAmount = 0;
  let tokenSymbol = '';
  let tokenMint = '';

  if (tradeInfo.type === 'buy') {
    // buy 类型：SOL 买入代币
    isSOLBuyToken = 
      (soldToken.mint === SOL_MINT || soldToken.mint === 'SOL') &&
      boughtToken.mint !== SOL_MINT && boughtToken.mint !== 'SOL';

    if (!isSOLBuyToken) {
      stats.skippedNotSOLBuy++;
      return null; // 不是 SOL 买入代币的交易，跳过
    }

    // 检查 SOL 金额是否大于阈值
    solAmount = soldToken.amount || 0;
    if (solAmount < MIN_SOL_AMOUNT) {
      stats.skippedLowAmount++;
      return null; // SOL 金额小于阈值，不记录
    }

    tokenSymbol = tokenMetadataMap.get(boughtToken.mint) || boughtToken.symbol || 'Unknown';
    tokenMint = boughtToken.mint;
  } else if (tradeInfo.type === 'swap') {
    // swap 类型：代币到代币交换
    isTokenSwap = 
      soldToken.mint !== SOL_MINT && soldToken.mint !== 'SOL' &&
      boughtToken.mint !== SOL_MINT && boughtToken.mint !== 'SOL';

    if (!isTokenSwap) {
      stats.skippedNotSOLBuy++;
      return null; // 不是代币到代币交换，跳过
    }

    // 判断是买入还是卖出：根据传入的 address（代币地址）判断
    // 如果 boughtToken.mint === address，则认为是买入
    // 如果 soldToken.mint === address，则认为是卖出（不保存）
    const normalizedAddress = (address || '').trim().toLowerCase();
    const normalizedBoughtMint = (boughtToken.mint || '').trim().toLowerCase();
    const normalizedSoldMint = (soldToken.mint || '').trim().toLowerCase();
    
    // 如果是卖出（soldToken.mint === address），跳过
    if (normalizedSoldMint === normalizedAddress) {
      stats.skippedNotBuy++;
      return null; // 卖出不记录
    }
    
    // 如果是买入（boughtToken.mint === address），保存
    if (normalizedBoughtMint === normalizedAddress) {
      // 对于 swap 买入，我们保存买入的代币信息
      // 由于 swap 没有直接的 SOL 金额，我们使用 0
      solAmount = 0; // swap 类型没有直接的 SOL 金额
      tokenSymbol = tokenMetadataMap.get(boughtToken.mint) || boughtToken.symbol || 'Unknown';
      tokenMint = boughtToken.mint;
    } else {
      // 如果 address 既不等于 boughtToken 也不等于 soldToken，跳过
      stats.skippedNotSOLBuy++;
      return null;
    }
  }

  // 获取持有人地址（优先使用 tradeInfo 中的 holderAddress）
  let holderAddress = tradeInfo.holderAddress || address;
  
  // 规范化地址：去除首尾空格（不做大小写转换，保持原始格式）
  holderAddress = (holderAddress || '').trim();
  
  // 如果地址为空，跳过
  if (!holderAddress) {
    stats.skippedNoHolder++;
    return null;
  }

  // 过滤机器人地址（需要转换为小写进行匹配，因为 robotAddresses 是小写的）
  if (robotAddresses.has(holderAddress.toLowerCase())) {
    stats.totalFilteredByRobot++;
    return null; // 跳过机器人地址的交易
  }

  // 获取交易时间（blockTime，Unix 时间戳，秒）
  const tradeAt = tx.blockTime || null;

  // 准备保存的数据
  const tradeData = {
    addr: holderAddress,
    splTag: tokenSymbol,
    splAddr: tokenMint,
    type: 1, // 1-买（buy 和 swap 都使用 type: 1）
    solAmount: parseFloat(solAmount.toFixed(2)),
    splAmount: parseFloat((boughtToken.amount || 0).toFixed(2)),
    tradeAt: tradeAt, // 使用实际的交易时间
    signature: signature // 交易签名
  };
  
  // 如果是 swap 类型，添加 swap_from_token 和 swap_from_tag
  if (tradeInfo.type === 'swap') {
    tradeData.swapFromToken = soldToken.mint;
    tradeData.swapFromTag = tokenMetadataMap.get(soldToken.mint) || soldToken.symbol || 'Unknown';
  }

  return tradeData;
}

/**
 * 处理一批交易数据
 */
async function processTransactionBatch(
  result,
  address,
  robotAddresses,
  blockTimeGte,
  blockTimeLt,
  SOL_MINT,
  MIN_SOL_AMOUNT,
  stats
) {
  // 解析交易买卖信息
  const { parseMultipleTradeInfoFromTransactions } = await import('../services/transactionParseService.js');
  const tradeInfoMap = await parseMultipleTradeInfoFromTransactions(result.data);
  const parsedCount = tradeInfoMap.size;
  const unparsedCount = result.data.length - parsedCount;
  console.log(`[后台任务] ✓ 解析出 ${parsedCount} 笔交易的买卖信息，${unparsedCount} 笔无法解析`);
  
  if (unparsedCount > 0 && unparsedCount > result.data.length * 0.5) {
    console.warn(`[后台任务] ⚠ 超过 50% 的交易无法解析，可能存在问题`);
  }

  // 收集所有代币地址，用于批量获取元数据
  const tokenMints = collectTokenMints(tradeInfoMap);
  
  // 批量获取代币元数据（符号）
  const tokenMetadataMap = await fetchTokenMetadata(tokenMints);

  // 当前批次符合条件的买入交易（立即保存，不累积）
  const currentBatchBuyTrades = [];

  // 处理每笔交易，找出买入交易
  result.data.forEach(tx => {
    const signature = tx.signature || tx.transaction?.signatures?.[0];
    const tradeInfo = tradeInfoMap.get(signature);
    
    const tradeData = processSingleTransaction(
      tx,
      tradeInfo,
      address,
      tokenMetadataMap,
      robotAddresses,
      blockTimeGte,
      blockTimeLt,
      SOL_MINT,
      MIN_SOL_AMOUNT,
      stats
    );

    if (tradeData) {
      currentBatchBuyTrades.push(tradeData);
      stats.totalBuyTrades++;
    }
  });

  return currentBatchBuyTrades;
}

/**
 * 批量获取代币元数据
 */
async function fetchTokenMetadata(tokenMints) {
  const tokenMetadataMap = new Map();
  if (tokenMints.size > 0) {
    try {
      const tokenAddresses = Array.from(tokenMints);
      const metadataResults = await getTokenMetadataMultipleViaHelius(
        config.helius.apiKey,
        tokenAddresses
      );
      metadataResults.forEach(metadata => {
        if (metadata && metadata.address) {
          tokenMetadataMap.set(metadata.address, metadata.symbol || 'Unknown');
        }
      });
      console.log(`[后台任务] ✓ 获取到 ${tokenMetadataMap.size} 个代币的符号`);
    } catch (error) {
      console.warn(`[后台任务] 批量获取代币元数据失败: ${error.message}`);
    }
  }
  return tokenMetadataMap;
}

/**
 * 收集所有代币地址
 */
function collectTokenMints(tradeInfoMap) {
  const tokenMints = new Set();
  tradeInfoMap.forEach(tradeInfo => {
    if (tradeInfo.soldToken?.mint) {
      tokenMints.add(tradeInfo.soldToken.mint);
    }
    if (tradeInfo.boughtToken?.mint) {
      tokenMints.add(tradeInfo.boughtToken.mint);
    }
  });
  return tokenMints;
}

/**
 * 记录最终统计信息
 */
function logFinalStatistics(
  totalTransactions,
  stats,
  totalBuyTrades,
  totalFilteredByRobot,
  totalSavedTrades,
  robotCount,
  MIN_SOL_AMOUNT,
  blockTimeGte,
  blockTimeLt
) {
  console.log(`[后台任务] ========== 最终统计 ==========`);
  console.log(`[后台任务] 总处理交易数: ${totalTransactions}`);
  console.log(`[后台任务] 过滤详情:`);
  console.log(`  - 无法解析交易: ${stats.totalSkippedNoTradeInfo}`);
  console.log(`  - 不是买入/交换交易: ${stats.totalSkippedNotBuy}`);
  console.log(`  - 不是SOL买入代币/代币交换: ${stats.totalSkippedNotSOLBuy}`);
  console.log(`  - SOL金额 < ${MIN_SOL_AMOUNT}: ${stats.totalSkippedLowAmount}`);
  console.log(`  - 无持有人地址: ${stats.totalSkippedNoHolder}`);
  console.log(`  - 时间范围外: ${stats.totalSkippedOutOfTimeRange}`);
  console.log(`  - 机器人地址过滤: ${totalFilteredByRobot}`);
  console.log(`[后台任务] 符合条件的买入/交换交易: ${totalBuyTrades}`);
  console.log(`[后台任务] 已保存到数据库: ${totalSavedTrades}`);
  console.log(`[后台任务] 过滤条件:`);
  console.log(`  - 只记录买入交易 (type === 'buy') 和交换交易 (type === 'swap')`);
  console.log(`  - buy: 只记录 SOL 买入代币的交易，SOL 金额 >= ${MIN_SOL_AMOUNT}`);
  console.log(`  - swap: 只记录代币到代币交换的交易`);
  console.log(`  - 过滤机器人地址 (${robotCount} 个)`);
  if (blockTimeGte && blockTimeLt) {
    console.log(`  - 时间范围: [${blockTimeGte}, ${blockTimeLt}]`);
  }
  console.log(`[后台任务] =============================`);
}

/**
 * 后台异步执行同步交易到数据库的逻辑
 * @param {string} address - 地址
 * @param {number} blockTimeGte - 开始时间（Unix 时间戳，秒，可选）
 * @param {number} blockTimeLt - 结束时间（Unix 时间戳，秒，可选）
 * @param {number} maxPages - 最大页数
 * @param {number} maxDuration - 最大持续时间（毫秒）
 */
async function syncTradesToDatabaseAsync(address, blockTimeGte, blockTimeLt, maxPages = 1000, maxDuration = 900000) {
  try {
    // 构建日志信息
    const filterInfo = [];
    if (blockTimeGte && blockTimeLt) {
      filterInfo.push(`时间范围 [${blockTimeGte}, ${blockTimeLt}]`);
    }
    const filterDesc = filterInfo.length > 0 ? filterInfo.join(', ') : '无过滤条件';
    console.log(`[后台任务] 开始同步地址 ${address} 在 ${filterDesc} 的交易到数据库...`);

    // 获取机器人地址列表（用于过滤）
    const robotAddresses = await getRobotAddresses();
    const robotCount = robotAddresses.size;
    if (robotCount > 0) {
      console.log(`[后台任务] ✓ 已加载 ${robotCount} 个机器人地址，将过滤这些地址的交易`);
    } else {
      console.log('[后台任务] ⚠ 未找到机器人地址列表，将保存所有符合条件的交易');
    }

    // 构建过滤条件
    const filters = buildSyncFilters(blockTimeGte, blockTimeLt);

    // 初始化统计变量
    const SOL_MINT = 'So11111111111111111111111111111111111111112';
    const MIN_SOL_AMOUNT = 0.5; // 最小 SOL 金额阈值
    
    let paginationToken = null;
    let totalTransactions = 0;
    let totalBuyTrades = 0;
    let totalFilteredByRobot = 0;
    let totalSavedTrades = 0;
    
    // 累计统计过滤原因
    const stats = {
      skippedNoTradeInfo: 0,
      skippedNotBuy: 0,
      skippedNotSOLBuy: 0,
      skippedLowAmount: 0,
      skippedNoHolder: 0,
      skippedOutOfTimeRange: 0,
      totalSkippedNoTradeInfo: 0,
      totalSkippedNotBuy: 0,
      totalSkippedNotSOLBuy: 0,
      totalSkippedLowAmount: 0,
      totalSkippedNoHolder: 0,
      totalSkippedOutOfTimeRange: 0,
      totalFilteredByRobot: 0
    };
    
    let pageCount = 0;
    const startTime = Date.now();

    // 循环获取所有交易
    do {
      // 检查是否超过最大页数
      if (pageCount >= maxPages) {
        console.log(`[后台任务] ⚠ 已达到最大页数限制 (${maxPages} 页)，停止同步`);
        break;
      }

      // 检查是否超过最大持续时间
      const elapsedTime = Date.now() - startTime;
      if (elapsedTime > maxDuration) {
        console.log(`[后台任务] ⚠ 已达到最大持续时间限制 (${maxDuration / 1000} 秒)，停止同步`);
        break;
      }

      pageCount++;
      console.log(`[后台任务] 正在获取第 ${pageCount} 页交易...`);
      
      let result;
      try {
        result = await getTransactionsForAddress(address, {
          limit: 100, // Helius API 限制：transactionDetails='full' 时最多 100
          transactionDetails: 'full', // 需要完整数据来解析买卖信息
          sortOrder: 'desc', // 按 slot 降序排列（从最新到最旧）
          filters,
          paginationToken,
          skipSystemInstructionFilter: true // 跳过系统指令过滤，获取所有交易，由业务逻辑决定是否保存
        });
      } catch (error) {
        console.error(`[后台任务] 获取第 ${pageCount} 页交易失败:`, error.message);
        throw error;
      }

      // 检查是否没有数据
      if (!result.data || result.data.length === 0) {
        console.log('[后台任务] 没有更多交易数据，停止同步');
        paginationToken = null; // 确保退出循环
        break;
      }

      // 检查返回的数据是否为空数组
      if (result.data.length === 0) {
        console.log('[后台任务] 返回的交易数据为空，停止同步');
        paginationToken = null;
        break;
      }

      totalTransactions += result.data.length;
      console.log(`[后台任务] ✓ 获取到 ${result.data.length} 笔交易，累计 ${totalTransactions} 笔 (第 ${pageCount}/${maxPages} 页)`);
      
      // 打印所有交易签名
      const signatures = result.data.map(tx => {
        const sig = tx.signature || tx.transaction?.signatures?.[0] || 'N/A';
        return sig;
      }).filter(sig => sig !== 'N/A');
      console.log(`[后台任务] 第 ${pageCount} 页所有交易签名 (共 ${signatures.length} 个):`);
      signatures.forEach((sig, index) => {
        console.log(`  ${index + 1}. ${sig}`);
      });
      
      // 统计：失败的交易数量
      const failedCount = result.data.filter(tx => tx.err || tx.meta?.err).length;
      if (failedCount > 0) {
        console.log(`[后台任务]   其中 ${failedCount} 笔失败的交易（将在后续过滤中排除）`);
      }

      // 重置本页统计
      stats.skippedNoTradeInfo = 0;
      stats.skippedNotBuy = 0;
      stats.skippedNotSOLBuy = 0;
      stats.skippedLowAmount = 0;
      stats.skippedNoHolder = 0;
      stats.skippedOutOfTimeRange = 0;

      // 处理当前批次的交易（包括解析、获取元数据、处理交易）
      const currentBatchBuyTrades = await processTransactionBatch(
        result,
        address,
        robotAddresses,
        blockTimeGte,
        blockTimeLt,
        SOL_MINT,
        MIN_SOL_AMOUNT,
        stats
      );

      // 如果当前批次有符合条件的交易，立即保存到数据库
      if (currentBatchBuyTrades.length > 0) {
        try {
          await batchSaveTradeInfo(currentBatchBuyTrades);
          const batchSavedCount = currentBatchBuyTrades.length;
          totalSavedTrades += batchSavedCount;
          console.log(`[后台任务] ✓ 第 ${pageCount} 页立即保存 ${batchSavedCount} 笔买入/交换交易到数据库（累计已保存 ${totalSavedTrades} 笔）`);
        } catch (error) {
          console.error(`[后台任务] 第 ${pageCount} 页保存交易信息失败:`, error);
          // 继续处理下一批，不中断整个流程
          console.warn(`[后台任务] ⚠ 第 ${pageCount} 页的 ${currentBatchBuyTrades.length} 笔交易保存失败，将继续处理下一批`);
        }
      }

      // 累计统计
      stats.totalSkippedNoTradeInfo += stats.skippedNoTradeInfo;
      stats.totalSkippedNotBuy += stats.skippedNotBuy;
      stats.totalSkippedNotSOLBuy += stats.skippedNotSOLBuy;
      stats.totalSkippedLowAmount += stats.skippedLowAmount;
      stats.totalSkippedNoHolder += stats.skippedNoHolder;
      stats.totalSkippedOutOfTimeRange += stats.skippedOutOfTimeRange;
      totalFilteredByRobot = stats.totalFilteredByRobot;

      // 更新分页令牌
      paginationToken = result.paginationToken;

      // 检查是否还有更多数据
      if (!paginationToken) {
        console.log('[后台任务] 已获取所有交易数据（没有更多分页令牌）');
        break;
      }

      // 检查返回的交易数量是否少于 limit（说明已经是最后一页）
      if (result.data.length < 100) {
        console.log(`[后台任务] 返回的交易数量 (${result.data.length}) 少于 limit (100)，可能是最后一页`);
      }

      // 添加短暂延迟，避免 API 速率限制
      await new Promise(resolve => setTimeout(resolve, 500));

    } while (paginationToken && pageCount < maxPages);

    // 所有批次处理完成，总结保存情况
    if (totalSavedTrades > 0) {
      console.log(`[后台任务] ✓ 所有批次处理完成，累计保存 ${totalSavedTrades} 笔买入/交换交易到数据库`);
    } else {
      console.log('[后台任务] 没有符合条件的买入/交换交易需要保存');
    }

    // 检查是否因为限制而停止
    const stoppedByLimit = pageCount >= maxPages || (Date.now() - startTime) > maxDuration;
    const message = stoppedByLimit 
      ? `同步部分完成（因达到限制而停止）`
      : '同步完成';

    console.log(`[后台任务] ${message}`);
    
    // 记录最终统计信息
    logFinalStatistics(
      totalTransactions,
      stats,
      totalBuyTrades,
      totalFilteredByRobot,
      totalSavedTrades,
      robotCount,
      MIN_SOL_AMOUNT,
      blockTimeGte,
      blockTimeLt
    );

    return {
      success: true,
      message,
      address,
      filters: {
        ...(blockTimeGte && blockTimeLt ? { blockTime: { gte: blockTimeGte, lte: blockTimeLt } } : {})
      },
      stats: {
        totalTransactions,
        totalBuyTrades,
        totalFilteredByRobot,
        totalSavedTrades,
        minSolAmount: MIN_SOL_AMOUNT,
        robotAddressCount: robotCount,
        pageCount,
        maxPages: maxPages,
        stoppedByLimit
      },
      paginationToken: stoppedByLimit ? paginationToken : null
    };
  } catch (error) {
    console.error('[后台任务] 同步交易到数据库失败:', error);
    throw error;
  }
}

/**
 * 同步交易到数据库的 HTTP 处理器
 * 立即返回响应，同步逻辑在后台异步执行
 */
export async function syncTradesToDatabaseHandler(req, res) {
  try {
    const { address } = req.params;
    let blockTimeGte = req.query.blockTimeGte ? parseInt(req.query.blockTimeGte) : null;
    let blockTimeLt = req.query.blockTimeLt ? parseInt(req.query.blockTimeLt) : null;

    // 现在页面可以传入北京时间 且 skipTimezoneConversion=true 跳过转换
    const skipTimezoneConversion = req.query.skipTimezoneConversion === 'true' || req.query.skipTimezoneConversion === '1';
    
    // 打印入口参数日志
    console.log('[syncTradesToDatabaseHandler] 地址 (address):', address);
    console.log('[syncTradesToDatabaseHandler] 开始时间 (blockTimeGte):', blockTimeGte);
    console.log('[syncTradesToDatabaseHandler] 结束时间 (blockTimeLt):', blockTimeLt);
    // 默认总是做时区转换（因为前端传入的通常都是北京时间）
    if (!skipTimezoneConversion && blockTimeGte && blockTimeLt) {
      const BEIJING_UTC_OFFSET = 8 * 60 * 60; // 8小时 = 28800秒
      
      // 转换公式：UTC时间戳 = 北京时间戳 - 28800秒
      blockTimeGte = blockTimeGte - BEIJING_UTC_OFFSET;
      blockTimeLt = blockTimeLt - BEIJING_UTC_OFFSET;
      
    }

    // 参数验证
    if (!address) {
      return res.status(400).json({
        success: false,
        error: '地址参数不能为空'
      });
    }

    // 必须提供时间过滤
    const hasBlockTimeFilter = blockTimeGte && blockTimeLt;

    if (!hasBlockTimeFilter) {
      return res.status(400).json({
        success: false,
        error: '必须提供时间过滤条件',
        options: [
          '提供 blockTimeGte 和 blockTimeLt（Unix 时间戳，秒）'
        ]
      });
    }

    if (!config.helius.apiKey) {
      return res.status(400).json({
        success: false,
        error: '需要配置 Helius API Key 才能使用此功能',
        hint: '请在 .env 文件中设置 HELIUS_API_KEY'
      });
    }

    // 获取可选参数
    const maxPages = parseInt(req.query.maxPages) || 500;
    const maxDuration = parseInt(req.query.maxDuration) || 900000;

    // 构建响应信息
    const filters = {
      blockTime: { gte: blockTimeGte, lte: blockTimeLt }
    };

    // 构建提示信息
    const notes = [];
    if (!skipTimezoneConversion) {
      notes.push('已自动将北京时间转换为 UTC 时间（减去 8 小时）');
    }
    if (notes.length === 0) {
      notes.push('请查看服务器日志了解同步进度和结果');
    }

    // 立即返回响应
    res.json({
      success: true,
      message: '同步任务已启动，正在后台执行',
      address,
      filters,
      timezone: skipTimezoneConversion ? 'UTC（链上时间）' : '已从北京时间转换为 UTC',
      note: notes.join('；')
    });

    // 在后台异步执行同步逻辑（不等待完成）
    syncTradesToDatabaseAsync(address, blockTimeGte, blockTimeLt, maxPages, maxDuration)
      .then(result => {
        console.log(`[后台任务] 同步完成:`, result);
      })
      .catch(error => {
        console.error(`[后台任务] 同步失败:`, error);
      });

  } catch (error) {
    console.error('启动同步任务失败:', error);
    res.status(500).json({
      success: false,
      error: error.message,
      hint: '请检查参数是否正确，或查看服务器日志获取详细信息'
    });
  }
}

/**
 * 同步分析持有者盈亏
 * 1. 从 tbl_sol_trade_info 表中归总代币和地址
 * 2. 对每个地址调用 Birdeye API 获取盈亏详情
 * 3. 根据过滤条件决定是否继续查询和保存
 */
export async function syncAnalyzeHolderPnlHandler(req, res) {
  try {
    if (!config.birdeye.apiKey) {
      return res.status(400).json({
        success: false,
        error: '未配置 Birdeye API Key',
        hint: '请在 .env 文件中设置 BIRDEYE_API_KEY'
      });
    }

    // 获取归总后的代币和地址
    const { getGroupedTokenAddresses } = await import('../db/solTradeInfoMapper.js');
    
    console.log('开始查询需要分析的地址...');
    const minAmount = parseInt(req.query.minAmount) || 1500000; // 支持通过查询参数自定义最小数量
    const records = await getGroupedTokenAddresses(minAmount);
    console.log(`查询到 ${records.length} 条记录（最小数量阈值: ${minAmount}）`);

    if (records.length === 0) {
      return res.json({
        success: true,
        message: '没有需要分析的记录',
        totalRecords: 0,
        processedCount: 0,
        savedCount: 0,
        skippedCount: 0
      });
    }

    // 统计信息
    let processedCount = 0;
    let savedCount = 0;
    let skippedCount = 0;
    const results = [];
    
    // 获取最小盈利阈值（支持通过查询参数自定义）
    const minPnlUsd = parseInt(req.query.minPnlUsd) || 5000;

    // 对每个地址进行处理
    for (const record of records) {
      const walletAddress = record.addr;
      const splAddr = record.spl_addr;
      const num = record.num;

      try {
        console.log(`\n处理地址 ${walletAddress} (代币: ${splAddr}, 数量: ${num})...`);

        // todo -lf
        // 调用内部函数获取 PnL 详情（启用过滤，不满足条件则停止）
        const pnlData = await getWalletPnlDetailsInternal(walletAddress, {
          limit: 100,
          maxPages: 100,
          enableFilter: true,  // 启用过滤
          saveToDatabase: true,  // 只有满足条件时才会保存（在函数内部判断）
          minPnlUsd: minPnlUsd  // 传入最小盈利阈值
        });

        processedCount++;

        if (pnlData.pagination.stoppedByFilter) {
          skippedCount++;
          results.push({
            walletAddress,
            splAddr,
            num,
            status: 'skipped',
            reason: '不满足过滤条件'
          });
          console.log(`  ⚠️  地址 ${walletAddress} 不满足过滤条件，已跳过`);
        } else {
          savedCount++;
          results.push({
            walletAddress,
            splAddr,
            num,
            status: 'saved',
            tokenCount: pnlData.pagination.totalTokens
          });
          console.log(`  ✓ 地址 ${walletAddress} 处理完成，保存了 ${pnlData.pagination.totalTokens} 个代币`);
        }

        // 添加延迟，避免请求过快
        await new Promise(resolve => setTimeout(resolve, 2000));
      } catch (error) {
        console.error(`处理地址 ${walletAddress} 失败:`, error.message);
        skippedCount++;
        results.push({
          walletAddress,
          splAddr,
          num,
          status: 'error',
          error: error.message
        });
      }
    }

    return res.json({
      success: true,
      message: '分析完成',
      totalRecords: records.length,
      processedCount,
      savedCount,
      skippedCount,
      results: results.slice(0, 100) // 只返回前100条结果
    });
  } catch (error) {
    console.error('同步分析持有者盈亏失败:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
}

