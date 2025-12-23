import { getTokenHolders } from './tokenService.js';
import { getTokenTotalSupplyWithCache } from './cacheService.js';
import { insertTokenHolders } from '../db/solAddrSplInfoMapper.js';
import { config } from '../config/index.js';
import { isKnownCEXAddress, isDEXProgramAddress, isLiquidityPoolAddress } from './addressFilter.js';
import { Connection } from '@solana/web3.js';

/**
 * 过滤流动性池和交易所地址
 * @param {Array} holders - 持有人列表
 * @param {boolean} filterPools - 是否过滤流动性池和交易所地址
 * @param {boolean} checkOnChain - 是否进行链上验证（较慢）
 * @param {Function} logger - 日志函数
 * @returns {Promise<{filteredHolders: Array, filteredCount: number}>} 过滤后的持有人列表和过滤数量
 */
export async function filterLiquidityPoolsAndExchanges(holders, filterPools, checkOnChain, logger) {
  let filteredHolders = holders;
  let filteredCount = 0;

  if (!filterPools || holders.length === 0) {
    return { filteredHolders, filteredCount };
  }

    logger(`正在过滤流动性池和交易所地址...`);
    
    // 快速过滤：先检查已知的 CEX 和 DEX 程序地址
    filteredHolders = holders.filter(holder => {
      // 跳过已知的 CEX 地址
      if (isKnownCEXAddress(holder.address)) {
        filteredCount++;
        return false;
      }
      // 跳过 DEX 程序地址
      if (isDEXProgramAddress(holder.address)) {
        filteredCount++;
        return false;
      }
      return true;
    });

    logger(`快速过滤后剩余 ${filteredHolders.length} 个地址（已过滤 ${filteredCount} 个）`);

    // 如果启用了链上检查，进一步验证
    if (checkOnChain && filteredHolders.length > 0) {
      logger(`正在进行链上验证（可能较慢）...`);
    const connection = new Connection(config.solana.rpcEndpoint, 'confirmed');
      
      const onChainFiltered = [];
      let onChainFilteredCount = 0;
      
      for (const holder of filteredHolders) {
        const isPool = await isLiquidityPoolAddress(holder.address, connection).catch(() => false);
        if (!isPool) {
          onChainFiltered.push(holder);
        } else {
          onChainFilteredCount++;
        }
      }
      
      filteredHolders = onChainFiltered;
      filteredCount += onChainFilteredCount;
      
      if (onChainFilteredCount > 0) {
        logger(`链上检查过滤了 ${onChainFilteredCount} 个流动性池地址`);
      }
  }

  return { filteredHolders, filteredCount };
}

/**
 * 同步代币持有人数据到数据库（公共方法）
 * @param {Object} options - 配置选项
 * @param {string} options.tokenAddress - 代币地址
 * @param {number} options.offset - 偏移量，默认 0
 * @param {number} options.limit - 限制数量，默认 100
 * @param {boolean} options.saveToDatabase - 是否保存到数据库，默认 true
 * @param {Function} options.logger - 日志函数，默认使用 console.log
 * @param {boolean} options.filterPools - 是否过滤流动性池和交易所地址，默认 false
 * @param {boolean} options.checkOnChain - 是否进行链上验证（较慢），默认 false
 * @param {boolean} options.enableClustering - 是否启用地址聚类（较慢，默认 false）
 * @returns {Promise<Object>} 同步结果
 */
export async function syncTokenHoldersData({
  tokenAddress,
  offset = 0,
  limit = 100,
  saveToDatabase = true,
  logger = console.log,
  filterPools = false,
  checkOnChain = false,
  enableClustering = false
}) {
  if (!tokenAddress) {
    throw new Error('代币地址不能为空');
  }

  logger(`开始同步代币 ${tokenAddress} 的持有人数据...`);

  // 获取持有人列表（不在这里进行地址聚类，会在过滤流动池之后执行）
  const holders = await getTokenHolders(
    tokenAddress,
    offset,
    limit
  );
  logger(`获取到 ${holders.length} 个持有人`);

  // 过滤流动性池和交易所地址
  let { filteredHolders, filteredCount } = await filterLiquidityPoolsAndExchanges(
    holders,
    filterPools,
    checkOnChain,
    logger
  );

  // 应用地址聚类（如果启用，在过滤流动池之后执行）
  if (enableClustering && filteredHolders.length > 0) {
    logger(`启用地址聚类分析（在过滤流动池之后）...`);
    try {
      const { mergeClusteredHolders } = await import('./addressClusteringService.js');
      filteredHolders = await mergeClusteredHolders(filteredHolders, true);
      logger(`地址聚类完成，剩余 ${filteredHolders.length} 个聚类后的地址`);
    } catch (error) {
      logger(`地址聚类失败: ${error.message}，继续使用未聚类的数据`);
    }
  }

  if (filteredHolders.length === 0) {
    logger(`未找到持有人数据`);
    return {
      success: true,
      message: '未找到持有人数据',
      tokenAddress,
      count: 0,
      totalSupply: 0,
      tokenSymbol: '',
      holders: [],
      totalSupplyInfo: {
        supply: '0',
        uiSupply: 0,
        decimals: 9,
        symbol: '',
        name: ''
      }
    };
  }

  // 获取代币总供应量和符号（使用缓存服务，先从 Redis 获取，取不到才调用 API）
  logger(`正在获取代币总供应量和符号（使用缓存）...`);
  let totalSupplyInfo;
  try {
    totalSupplyInfo = await getTokenTotalSupplyWithCache(
      config.helius.apiKey || null,
      tokenAddress
    );
    logger(`代币符号: ${totalSupplyInfo.symbol || '(未获取)'}, 总供应量: ${totalSupplyInfo.uiSupply}`);
  } catch (error) {
    logger(`获取代币总供应量失败: ${error.message}`);
    const calculatedSupply = holders.reduce((sum, holder) => sum + (holder.uiAmount || 0), 0);
    totalSupplyInfo = {
      supply: '0',
      uiSupply: calculatedSupply,
      decimals: 9,
      symbol: '',
      name: '',
    };
    logger(`使用计算的总供应量: ${calculatedSupply}`);
  }

  // 计算百分比并格式化持有人数据
  logger(`正在格式化持有人数据...`);
  const totalSupply = totalSupplyInfo.uiSupply;
  const tokenSymbol = totalSupplyInfo.symbol || '';

  const formattedHolders = filteredHolders.map((holder, index) => ({
    rank: holder.rank !== null ? holder.rank : offset + index + 1,
    address: holder.address,
    amount: holder.amount,
    uiAmount: holder.uiAmount,
    percentage: totalSupply > 0 ? ((holder.uiAmount / totalSupply) * 100).toFixed(4) : '0.0000',
  }));

  // 保存到数据库
  if (saveToDatabase) {
    logger(`正在保存 ${formattedHolders.length} 条数据到数据库...`);
    await insertTokenHolders(formattedHolders, tokenAddress, tokenSymbol);
    logger(`数据保存完成`);
  }

  return {
    success: true,
    message: '同步完成',
    tokenAddress,
    tokenSymbol,
    count: formattedHolders.length,
    filtered: filterPools ? filteredCount : 0,
    totalSupply: totalSupply,
    holders: formattedHolders,
    totalSupplyInfo: {
      supply: totalSupplyInfo.supply,
      uiSupply: totalSupplyInfo.uiSupply,
      decimals: totalSupplyInfo.decimals,
      symbol: totalSupplyInfo.symbol,
      name: totalSupplyInfo.name
    }
  };
}

