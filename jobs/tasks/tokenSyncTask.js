import { getTokenHolders } from '../../services/tokenService.js';
import { getTokenTotalSupplyWithCache } from '../../services/cacheService.js';
import { insertTokenHolders } from '../../db/solAddrSplInfoMapper.js';
import { config } from '../../config/index.js';
import { isKnownCEXAddress, isDEXProgramAddress, isLiquidityPoolAddress, isKnownLiquidityPoolAddress } from '../../services/addressFilter.js';
import { Connection } from '@solana/web3.js';
import { getLiquidityPools, saveLiquidityPools, hasLiquidityPools } from '../../services/liquidityPoolStorage.js';

/**
 * 获取代币总供应量和符号信息
 * @param {string} tokenAddress - 代币地址
 * @param {Function} logger - 日志函数
 * @returns {Promise<Object>} 代币总供应量信息
 */
async function getTokenSupplyInfo(tokenAddress, logger) {
  logger(`正在获取代币总供应量和符号（使用缓存）...`);
  try {
    const totalSupplyInfo = await getTokenTotalSupplyWithCache(
      config.helius.apiKey || null,
      tokenAddress
    );
    logger(`代币符号: ${totalSupplyInfo.symbol || '(未获取)'}, 总供应量: ${totalSupplyInfo.uiSupply}`);
    return totalSupplyInfo;
  } catch (error) {
    logger(`获取代币总供应量失败: ${error.message}`);
    throw new Error(`无法获取代币信息: ${error.message}`);
  }
}

/**
 * 检查前N个地址中的流动性池（仅在第一次运行时执行）
 * @param {Array} holders - 持有人列表（前10个）
 * @param {string} tokenAddress - 代币地址
 * @param {Function} logger - 日志函数
 * @returns {Promise<Array<string>>} 识别到的流动性池地址列表
 */
async function detectLiquidityPoolsFromTopHolders(holders, tokenAddress, logger) {
  if (!holders || holders.length === 0) {
    return [];
  }

  logger(`正在检查前 ${holders.length} 个地址中的流动性池...`);
  const connection = new Connection(config.solana.rpcEndpoint, 'confirmed');
  const detectedPools = [];

  for (const holder of holders) {
    try {
      const isPool = await isLiquidityPoolAddress(holder.address, connection).catch(() => false);
      if (isPool) {
        detectedPools.push(holder.address);
        logger(`发现流动性池地址: ${holder.address}`);
      }
    } catch (error) {
      logger(`检查地址 ${holder.address} 失败: ${error.message}`);
    }
  }

  if (detectedPools.length > 0) {
    logger(`在前 ${holders.length} 个地址中识别到 ${detectedPools.length} 个流动性池地址`);
    // 保存到数据库或 Redis
    await saveLiquidityPools(tokenAddress, detectedPools);
  } else {
    logger(`在前 ${holders.length} 个地址中未发现流动性池地址`);
  }

  return detectedPools;
}

/**
 * 过滤流动性池和交易所地址
 * @param {Array} holders - 持有人列表
 * @param {boolean} filterPools - 是否过滤流动性池
 * @param {string} tokenAddress - 代币地址
 * @param {boolean} isFirstRun - 是否是第一次运行
 * @param {Set<string>} knownPoolAddresses - 已知的流动性池地址集合（从数据库/Redis加载）
 * @param {Function} logger - 日志函数
 * @returns {Promise<{filteredHolders: Array, filteredCount: number}>} 过滤后的持有人和过滤数量
 */
async function filterLiquidityPoolHolders(holders, filterPools, tokenAddress, isFirstRun, knownPoolAddresses, logger) {
  if (!filterPools) {
    return {
      filteredHolders: holders,
      filteredCount: 0
    };
  }

  logger(`正在过滤流动性池和交易所地址...`);
  
  // 快速过滤：先检查已知的 CEX 和 DEX 程序地址，以及已保存的流动性池地址
  let filteredHolders = holders.filter(holder => {
    // 跳过已知的 CEX 地址
    if (isKnownCEXAddress(holder.address)) {
      return false;
    }
    // 跳过已知的流动性池地址（配置文件中的）
    if (isKnownLiquidityPoolAddress(holder.address)) {
      return false;
    }
    // 跳过已保存的流动性池地址（从数据库/Redis加载的）
    if (knownPoolAddresses.has(holder.address)) {
      return false;
    }
    // 跳过 DEX 程序地址
    if (isDEXProgramAddress(holder.address)) {
      return false;
    }
    return true;
  });

  const quickFilteredCount = holders.length - filteredHolders.length;
  logger(`快速过滤后剩余 ${filteredHolders.length} 个地址（已过滤 ${quickFilteredCount} 个）`);

  return {
    filteredHolders,
    filteredCount: quickFilteredCount
  };
}

/**
 * 格式化持有人数据并按持仓百分比过滤
 * @param {Array} holders - 持有人列表
 * @param {number} totalSupply - 代币总供应量
 * @param {number} offset - 当前偏移量
 * @param {number} minPercentage - 最小持仓百分比阈值
 * @returns {Object} {holdersToSave: Array, holdersToSkip: Array}
 */
function formatAndFilterHoldersByPercentage(holders, totalSupply, offset, minPercentage) {
  const holdersToSave = [];
  const holdersToSkip = [];

  for (let i = 0; i < holders.length; i++) {
    const holder = holders[i];
    const percentage = totalSupply > 0 ? ((holder.uiAmount / totalSupply) * 100) : 0;

    const formattedHolder = {
      rank: holder.rank !== null ? holder.rank : offset + i + 1,
      address: holder.address,
      amount: holder.amount,
      uiAmount: holder.uiAmount,
      percentage: percentage.toFixed(4),
    };

    if (percentage >= minPercentage) {
      holdersToSave.push(formattedHolder);
    } else {
      holdersToSkip.push(formattedHolder);
    }
  }

  return { holdersToSave, holdersToSkip };
}

/**
 * 生成批次时间（格式：2025-11-30 12:05:00，精确到分钟）
 * @returns {string} 批次时间字符串
 */
function generateBatchTime() {
  const now = new Date();
  const year = now.getFullYear();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  const hours = String(now.getHours()).padStart(2, '0');
  const minutes = String(now.getMinutes()).padStart(2, '0');
  return `${year}-${month}-${day} ${hours}:${minutes}:00`;
}

/**
 * 保存持有人数据到数据库
 * @param {Array} holdersToSave - 要保存的持有人列表
 * @param {string} tokenAddress - 代币地址
 * @param {string} tokenSymbol - 代币符号
 * @param {string} batchTime - 批次时间
 * @param {Function} logger - 日志函数
 */
async function saveHoldersToDatabase(holdersToSave, tokenAddress, tokenSymbol, batchTime, logger) {
  if (holdersToSave.length > 0) {
    logger(`保存 ${holdersToSave.length} 条数据到数据库 (持仓百分比 >= 0.2%, 批次时间: ${batchTime})`);
    await insertTokenHolders(holdersToSave, tokenAddress, tokenSymbol, batchTime);
  }
}

/**
 * 判断是否应该继续查询下一页
 * @param {number} holdersToSkipCount - 跳过的持有人数量
 * @param {number} holdersCount - 当前页持有人数量
 * @param {number} pageSize - 每页大小
 * @returns {boolean} 是否继续查询
 */
function shouldContinueFetching(holdersToSkipCount, holdersCount, pageSize) {
  // 如果当前页有任何一个持有人的百分比小于阈值，说明后续的都会更小，可以停止
  if (holdersToSkipCount > 0) {
    return false;
  }
  // 如果返回的数据少于页面大小，说明已经是最后一页
  if (holdersCount < pageSize) {
    return false;
  }
  return true;
}

/**
 * 同步代币持有人数据任务
 * 分页查询，每次100个地址，循环查询直到持仓百分比小于0.2%
 * 持仓百分比小于0.2%的地址不存入数据库
 * @param {Object} job - BullMQ 任务对象
 * @returns {Promise<Object>} 任务结果
 */
export async function syncTokenHoldersJob(job) {
  const { tokenAddress, filterPools = true } = job.data;

  if (!tokenAddress) {
    throw new Error('代币地址不能为空');
  }

  try {
    const logger = (message) => console.log(`[任务 ${job.id}] ${message}`);
    const MIN_PERCENTAGE = 0.2; // 最小持仓百分比阈值
    const PAGE_SIZE = 100; // 每页查询数量

    logger(`开始同步代币 ${tokenAddress} 的持有人数据...`);

    // 需求2：生成批次时间（每次 job 执行时生成，同一批次的所有数据使用相同的 batch_time）
    const batchTime = generateBatchTime();
    logger(`批次时间: ${batchTime}`);

    // 需求1.2：在任务内部检查是否是第一次运行（通过检查是否已有流动性池地址记录）
    const isFirstRun = !(await hasLiquidityPools(tokenAddress));
    
    // 加载已保存的流动性池地址
    let knownPoolAddresses = new Set();
    if (isFirstRun) {
      logger(`这是第一次运行，将检查前10个地址中的流动性池`);
    } else {
      logger(`非第一次运行，加载已保存的流动性池地址列表`);
      const savedPools = await getLiquidityPools(tokenAddress);
      knownPoolAddresses = new Set(savedPools);
      logger(`已加载 ${knownPoolAddresses.size} 个已保存的流动性池地址`);
    }

    // 1. 获取代币总供应量和符号
    const totalSupplyInfo = await getTokenSupplyInfo(tokenAddress, logger);
    const totalSupply = totalSupplyInfo.uiSupply;
    const tokenSymbol = totalSupplyInfo.symbol || '';

    if (totalSupply <= 0) {
      throw new Error('代币总供应量为0，无法计算持仓百分比');
    }

    // 需求1.2：如果是第一次运行，检查前10个地址中的流动性池
    if (isFirstRun) {
      logger(`获取前10个地址进行流动性池检测...`);
      const topHolders = await getTokenHolders(
        tokenAddress,
        0,
        10 // 只获取前10个
      );
      
      if (topHolders.length > 0) {
        const detectedPools = await detectLiquidityPoolsFromTopHolders(topHolders, tokenAddress, logger);
        // 将识别到的流动性池地址添加到已知列表中
        detectedPools.forEach(addr => knownPoolAddresses.add(addr));
        logger(`流动性池检测完成，共识别 ${detectedPools.length} 个地址`);
      }
    }

    // 2. 分页循环查询
    let offset = 0;
    let totalSaved = 0;
    let totalSkipped = 0;
    let totalFiltered = 0;
    let shouldContinue = true;

    while (shouldContinue) {
      logger(`正在获取第 ${Math.floor(offset / PAGE_SIZE) + 1} 页持有人数据 (offset: ${offset}, limit: ${PAGE_SIZE})...`);

      // 2.1 获取当前页的持有人列表
      const holders = await getTokenHolders(
        tokenAddress,
        offset,
        PAGE_SIZE
      );

      if (holders.length === 0) {
        logger(`第 ${Math.floor(offset / PAGE_SIZE) + 1} 页没有更多数据，停止查询`);
        break;
      }

      logger(`获取到 ${holders.length} 个持有人`);

      // 2.2 过滤流动性池和交易所地址（使用已保存的流动性池地址列表）
      const { filteredHolders, filteredCount } = await filterLiquidityPoolHolders(
        holders,
        filterPools,
        tokenAddress,
        isFirstRun,
        knownPoolAddresses, // 传入已保存的流动性池地址集合
        logger
      );
      totalFiltered += filteredCount;

      // 2.3 格式化持有人数据并按持仓百分比过滤
      const { holdersToSave, holdersToSkip } = formatAndFilterHoldersByPercentage(
        filteredHolders,
        totalSupply,
        offset,
        MIN_PERCENTAGE
      );

      // 2.4 保存符合条件的持有人到数据库（需求2：传入批次时间）
      await saveHoldersToDatabase(holdersToSave, tokenAddress, tokenSymbol, batchTime, logger);
      totalSaved += holdersToSave.length;

      if (holdersToSkip.length > 0) {
        logger(`跳过 ${holdersToSkip.length} 条数据 (持仓百分比 < ${MIN_PERCENTAGE}%)`);
        totalSkipped += holdersToSkip.length;
      }

      // 2.5 判断是否继续查询
      shouldContinue = shouldContinueFetching(holdersToSkip.length, holders.length, PAGE_SIZE);
      
      if (!shouldContinue && holdersToSkip.length > 0) {
        logger(`检测到持仓百分比 < ${MIN_PERCENTAGE}% 的地址，停止查询`);
      } else if (!shouldContinue) {
        logger(`已获取所有数据，停止查询`);
      } else {
        // 继续查询下一页
        offset += PAGE_SIZE;
      }
    }

    logger(`同步完成: 共保存 ${totalSaved} 条，跳过 ${totalSkipped} 条${filterPools ? `，过滤流动性池/交易所 ${totalFiltered} 个` : ''}`);

    return {
      success: true,
      message: '同步完成',
      tokenAddress,
      tokenSymbol,
      count: totalSaved,
      skipped: totalSkipped,
      filtered: filterPools ? totalFiltered : 0,
      totalSupply: totalSupply,
    };
  } catch (error) {
    console.error(`[任务 ${job.id}] 同步失败:`, error);
    throw error;
  }
}
