import {config} from '../config/index.js';
import {getSolUsdPrice, getTokenMetadataMultipleViaHelius} from '../services/cacheService.js';
import {batchSavePnlInfo, checkPnlInfoExist} from '../db/solAddrPnlInfoMapper.js';
import {getZhiShouAddr, saveAndMarkAddr} from "../db/solTradeInfoMapper.js";


// ===================== 常量定义 =====================
const SOL_MINT = 'So11111111111111111111111111111111111111112';
const STABLE_COINS = {
    USDC: 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v',
    USDT: 'Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB',
    USDC_LEGACY: 'A9mUU4qviSctJVPJdBJWkb28deg915LYJKrzQ19ji3FM',
};
const STABLE_COIN_MINTS = Object.values(STABLE_COINS);
const FIRST_PAGE_MAX_TRANSACTIONS = 200;
const MAX_BATCH_COUNT = 2000; // 2000笔交易统计一次，分批来

// ===================== 工具函数 =====================
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function isCAMint(mint) {
    return mint && mint !== SOL_MINT && !STABLE_COIN_MINTS.includes(mint);
}

function addToMap(map, key, value) {
    if (!map.has(key)) {
        map.set(key, []);
    }
    map.get(key).push(value);
}

function markPriceUnit(swapInfo) {
    if (swapInfo.price !== null && swapInfo.price !== undefined) {
        swapInfo.priceUnit = 'USD';
    }
}

function enrichTokenSymbols(swapInfo, tokenMetadataMap) {
    const { fromToken, toToken } = swapInfo;

    if (fromToken?.mint) {
        const symbol = tokenMetadataMap.get(fromToken.mint);
        if (symbol) fromToken.symbol = symbol;
    }

    if (toToken?.mint) {
        const symbol = tokenMetadataMap.get(toToken.mint);
        if (symbol) toToken.symbol = symbol;
    }
}

// ===================== 数据获取函数 =====================
async function getTransactionSignatures(walletAddress, apiKey, options = {}) {
    const {
        transactionDetails = 'signatures',
        sortOrder = 'desc',
        paginationToken = null
    } = options;

    const heliusRpcUrl = `https://mainnet.helius-rpc.com/?api-key=${apiKey}`;

    const configObj = {
        transactionDetails,
        limit: 1000,
        sortOrder
    };

    if (paginationToken) {
        configObj.paginationToken = paginationToken;
    }

    const requestBody = {
        jsonrpc: '2.0',
        id: 'helius-signature-lookup',
        method: 'getTransactionsForAddress',
        params: [walletAddress, configObj]
    };

    const response = await fetch(heliusRpcUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody),
    });

    if (!response.ok) {
        const errorText = await response.text();
        throw new Error(`Helius RPC 请求失败: HTTP ${response.status} - ${errorText}`);
    }

    const result = await response.json();

    if (result.error) {
        throw new Error(`Helius RPC 错误: ${JSON.stringify(result.error)}`);
    }

    const transactions = result.result?.data || [];
    const nextPaginationToken = result.result?.paginationToken;

    let shouldContinue = true;

    if (transactions.length > 0) {
        const latestTransaction = transactions[0];
        const blockTime = latestTransaction.blockTime;

        if (blockTime) {
            const oneYearAgo = Math.floor(Date.now() / 1000) - (365 * 24 * 60 * 60);
            if (blockTime < oneYearAgo) {
                shouldContinue = false;
                console.log(`最新交易时间: ${new Date(blockTime * 1000).toLocaleString()}, 已超过一年，停止查询`);
            } else {
                console.log(`最新交易时间: ${new Date(blockTime * 1000).toLocaleString()}, 在一年内，继续查询`);
            }
        }
    } else {
        shouldContinue = false;
        console.log('没有找到交易记录，停止查询');
    }

    return {
        transactions,
        paginationToken: shouldContinue ? nextPaginationToken : null
    };
}

async function getEnhancedTransactions(signatures, apiKey) {
    const apiUrl = `https://api.helius.xyz/v0/transactions?api-key=${apiKey}`;
    const batchSize = 100;
    const batches = [];

    for (let i = 0; i < signatures.length; i += batchSize) {
        batches.push(signatures.slice(i, i + batchSize));
    }

    const allTransactions = [];

    for (let i = 0; i < batches.length; i++) {
        const batch = batches[i];
        const requestBody = { transactions: batch };

        try {
            const response = await fetch(apiUrl, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody),
            });

            if (!response.ok) {
                const errorText = await response.text();
                console.error(`批次 ${i + 1} 请求失败: HTTP ${response.status}`, errorText.substring(0, 200));
                continue;
            }

            const transactions = await response.json();
            if (Array.isArray(transactions)) {
                for (const tx of transactions) {
                    if (tx.type === 'SWAP') {
                        allTransactions.push(tx);
                    }
                }
            } else {
                console.warn(`批次 ${i + 1} 返回了非数组格式数据:`, transactions);
            }
        } catch (error) {
            console.error(`获取批次 ${i + 1} 时发生网络错误:`, error.message);
        }

        if (i < batches.length - 1) {
            await sleep(100);
        }
    }

    return allTransactions;
}

async function fetchSwapTransactionsByHeliusSwapType(walletAddress, apiKey, nextPaginationToken = null, isFirstQuery) {
    let allTransactions = [];
    let transactionType = "SWAP";
    let pageCount = 0;
    const limit = 100;
    let before = nextPaginationToken;
    let transferCount = 0;


    while (true) {
        pageCount++;

        const urlParams = new URLSearchParams();
        urlParams.append('type', transactionType);
        urlParams.append('limit', limit.toString());
        if (before != null) {
            urlParams.append("before", before);
        }

        const queryString = urlParams.toString();
        const apiUrl = `https://api.helius.xyz/v0/addresses/${walletAddress}/transactions?api-key=${apiKey}&${queryString}`;

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

            if (response.status === 404) {
                if (transferCount < 2) {
                    transactionType = "TRANSFER";
                    transferCount += 1;
                    console.log(`查询swap为空,第: ${transferCount} 次`);
                    continue;
                } else {
                    console.error(`地址: ${walletAddress} transfer信息过多，看看是否单独验证地址盈利详情`);
                    before = null;  // 这个地方置为null, 不然外层循环函数会一直调用
                    break;
                }
            }
        }

        let transactions = await response.json();

        if (transactions != null && transactions.length > 0) {

            if (transactionType === "SWAP") {
                allTransactions.push(...transactions);
            }

            transactions.sort((a, b) => b.timestamp - a.timestamp);
            before = transactions[transactions.length - 1].signature;
            transactionType = "SWAP";

            // 这个地方做一下时间过滤， 超过1年的时间不再查询
            const latestBlockTime = transactions[transactions.length - 1].timestamp;
            // 计算一年前的 Unix 时间戳（秒）(250天）
            const currentTimestamp = Math.floor(Date.now() / 1000); // 当前时间戳（秒）
            const oneYearInSeconds = 250 * 24 * 60 * 60; // 一年的秒数

            // 如果交易时间早于一年前，停止查询
            if (latestBlockTime < (currentTimestamp - oneYearInSeconds)) {
                before = null;
                break; // 如果在循环中，可以直接 break
            }
        }

        // 第一次查询不查询那么多数据，先出去判断是否纸手，非纸手才统计
        if (isFirstQuery === 1 && allTransactions.length >= FIRST_PAGE_MAX_TRANSACTIONS) {
            console.log(`✅ 第一次查询，交易笔数已达到最大限制 ${FIRST_PAGE_MAX_TRANSACTIONS} 笔，停止查询`);
            console.log(`  当前交易笔数: ${allTransactions.length}`);
            break;
        }

        // 这个地方和上面一样，before 没有置为null, 外层再循环过滤一样会再重新查询，目的是分批插入数据库
        if (allTransactions.length >= MAX_BATCH_COUNT) {
            console.log(`✅ 该批次交易笔数已达到最大限制 ${MAX_BATCH_COUNT} 笔`);
            console.log(`  当前交易笔数: ${allTransactions.length}`);
            break;
        }

        // 这个地方要把 before 设置为null，不然外层会一直调用
        if (transactions.length === 0) {
            before = null;
            break;
        }

        if (before == null) {
            break;
        }

        await sleep(100);
    }

    return {
        transactions: allTransactions,
        nextPaginationToken: before,
        hasMore: before !== null
    };
}

async function fetchSwapTransactions(walletAddress, apiKey, paginationToken = null) {
    console.log(`🚀 开始为地址 ${walletAddress}... 获取交易`);

    let signatureBatch = [];
    let nextPaginationToken = null;

    try {
        const { transactions, paginationToken: returnedToken } = await getTransactionSignatures(
            walletAddress,
            apiKey,
            { paginationToken: paginationToken }
        );

        if (!transactions || transactions.length === 0) {
            return {
                transactions: [],
                nextPaginationToken: null,
                hasMore: false
            };
        }

        nextPaginationToken = returnedToken;
        signatureBatch = transactions.map(tx => tx.signature).filter(sig => sig);
    } catch (error) {
        console.error('❌ 获取交易签名失败:', error.message);
        throw new Error(`获取交易列表失败: ${error.message}`);
    }

    let enhancedTransactions = [];
    try {
        enhancedTransactions = await getEnhancedTransactions(signatureBatch, apiKey);
    } catch (error) {
        console.error('❌ 批量获取交易详情失败:', error.message);
    }

    return {
        transactions: enhancedTransactions,
        nextPaginationToken: nextPaginationToken,
        hasMore: nextPaginationToken !== null
    };
}

function parseApiResponse(response) {
    if (Array.isArray(response)) {
        return response;
    }
    if (response.transactions && Array.isArray(response.transactions)) {
        console.warn('⚠️  收到旧格式响应，请确认API端点。');
        return response.transactions;
    }
    console.warn('⚠️  未知的响应格式，返回空数组:', response);
    return [];
}

// ===================== 交易解析函数 =====================
async function parseSwapFromHeliusTransaction(transaction, walletAddress) {
    try {
        if (!transaction || transaction.type !== 'SWAP') {
            return null;
        }

        const tokenTransfers = transaction.tokenTransfers || [];
        const nativeTransfers = transaction.nativeTransfers || [];
        const accountData = transaction.accountData || [];

        const tokenBalanceChanges = [];
        let userSolChange = 0;

        for (const account of accountData) {
            if (account.account === walletAddress && account.nativeBalanceChange !== undefined) {
                userSolChange = account.nativeBalanceChange / 1e9;
            }

            if (account.tokenBalanceChanges && account.tokenBalanceChanges.length > 0) {
                for (const change of account.tokenBalanceChanges) {
                    if (change.userAccount === walletAddress) {
                        const rawAmount = BigInt(change.rawTokenAmount?.tokenAmount || '0');
                        const decimals = change.rawTokenAmount?.decimals || 0;
                        const amount = Number(rawAmount) / Math.pow(10, decimals);

                        tokenBalanceChanges.push({
                            mint: change.mint,
                            userAccount: change.userAccount,
                            amount: amount,
                            rawAmount: rawAmount.toString(),
                            decimals: decimals
                        });
                    }
                }
            }
        }

        if (tokenBalanceChanges.length === 0) {
            const mintMap = new Map();
            for (const transfer of tokenTransfers) {
                const mint = transfer.mint;
                if (!mintMap.has(mint)) {
                    mintMap.set(mint, {
                        mint: mint,
                        amount: 0,
                        decimals: 9
                    });
                }
                const amount = parseFloat(transfer.tokenAmount) || 0;
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

        const soldToken = tokenBalanceChanges.find(change => change.amount < 0);
        const boughtToken = tokenBalanceChanges.find(change => change.amount > 0);

        if (Math.abs(userSolChange) < 0.0001) {
            for (const transfer of nativeTransfers) {
                const amount = transfer.amount / 1e9;
                if (transfer.fromUserAccount === walletAddress) {
                    userSolChange -= amount;
                } else if (transfer.toUserAccount === walletAddress) {
                    userSolChange += amount;
                }
            }
        }

        let fromToken = null;
        let toToken = null;
        let price = null;
        let swapType = 'swap';

        if (soldToken && boughtToken) {
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

        if (fromToken && toToken) {
            if (fromToken.mint === SOL_MINT) {
                swapType = 'buy';
            } else if (toToken.mint === SOL_MINT) {
                swapType = 'sell';
            }

            if (fromToken.amount > 0) {
                if (toToken.mint === SOL_MINT) {
                    const solPrice = await getSolUsdPrice();
                    if (solPrice && solPrice > 0) {
                        const toTokenUsdValue = toToken.amount * solPrice;
                        price = toTokenUsdValue / fromToken.amount;
                    }
                } else if (STABLE_COIN_MINTS.includes(toToken.mint)) {
                    price = toToken.amount / fromToken.amount;
                } else if (STABLE_COIN_MINTS.includes(fromToken.mint)) {
                    price = fromToken.amount / toToken.amount;
                } else if (fromToken.mint === SOL_MINT) {
                    const solPrice = await getSolUsdPrice();
                    if (solPrice && solPrice > 0) {
                        const fromTokenUsdValue = fromToken.amount * solPrice;
                        price = fromTokenUsdValue / toToken.amount;
                    } else {
                        price = null;
                    }
                } else {
                    try {
                        const tokenAddresses = [fromToken.mint, toToken.mint];
                        // 注意：这里需要实现 getTokenPricesFromBirdeye 或替换为相应的方法
                        // const priceResults = await getTokenPricesFromBirdeye(tokenAddresses);
                        // const priceMap = new Map(priceResults.map(r => [r.address, r.price]));

                        // 临时处理：如果没有实现价格获取，设为null
                        price = null;
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
            fromToken: fromToken,
            toToken: toToken,
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

async function parseAllSwaps(transactions, walletAddress) {
    const parsedSwapsPromises = transactions.map(async (tx) => {
        const swapInfo = await parseSwapFromHeliusTransaction(tx, walletAddress);
        if (!swapInfo) return null;

        return {
            signature: tx.signature || swapInfo.signature,
            swapInfo
        };
    });

    return (await Promise.all(parsedSwapsPromises)).filter(tx => tx !== null);
}

// ===================== 元数据处理函数 =====================
async function fetchTokenMetadata(parsedSwaps, apiKey) {
    const tokenMints = new Set();
    parsedSwaps.forEach(tx => {
        const { fromToken, toToken } = tx.swapInfo;
        if (fromToken?.mint) tokenMints.add(fromToken.mint);
        if (toToken?.mint) tokenMints.add(toToken.mint);
    });

    const tokenMetadataMap = new Map();

    if (tokenMints.size > 0) {
        try {
            const tokenAddresses = Array.from(tokenMints);
            const metadataResults = await getTokenMetadataMultipleViaHelius(apiKey, tokenAddresses);

            metadataResults.forEach(metadata => {
                if (metadata && metadata.address) {
                    tokenMetadataMap.set(metadata.address, metadata.symbol || 'Unknown');
                }
            });
        } catch (error) {
            console.warn(`批量获取代币元数据失败: ${error.message}，将使用默认符号`);
        }
    }

    return tokenMetadataMap;
}

// ===================== 分组和计算函数 =====================
function groupTransactionsByType(parsedSwaps, tokenMetadataMap, solPrice) {
    const sellMap = new Map();
    const buyMap = new Map();

    for (const tx of parsedSwaps) {
        const swapInfo = tx.swapInfo;
        if (!swapInfo) continue;

        enrichTokenSymbols(swapInfo, tokenMetadataMap);
        markPriceUnit(swapInfo);

        const swapType = swapInfo.type;
        const { fromToken, toToken } = swapInfo;

        if (swapType === 'sell' && isCAMint(fromToken?.mint)) {
            addToMap(sellMap, fromToken.mint, swapInfo);
        } else if (swapType === 'buy' && isCAMint(toToken?.mint)) {
            addToMap(buyMap, toToken.mint, swapInfo);
        }
    }

    return { sellMap, buyMap };
}

function summarizeSwaps(swapList, type, solPrice) {
    let totalQuantity = 0;
    let totalAmount = 0;
    const swaps = [];

    for (const swapInfo of swapList) {
        const { fromToken, toToken, price, signature, timestamp } = swapInfo;

        let amountUsd = 0;
        let quantity = 0;

        if (type === 'sell') {
            quantity = fromToken?.amount || 0;
            const toTokenMint = toToken?.mint;
            const toTokenAmount = toToken?.amount || 0;

            if (toTokenMint === SOL_MINT && solPrice > 0) {
                amountUsd = toTokenAmount * solPrice;
            } else if (STABLE_COIN_MINTS.includes(toTokenMint)) {
                amountUsd = toTokenAmount;
            } else if (price > 0) {
                amountUsd = quantity * price;
            }
        } else {
            quantity = toToken?.amount || 0;
            const fromTokenMint = fromToken?.mint;
            const fromTokenAmount = fromToken?.amount || 0;

            if (fromTokenMint === SOL_MINT && solPrice > 0) {
                amountUsd = fromTokenAmount * solPrice;
            } else if (STABLE_COIN_MINTS.includes(fromTokenMint)) {
                amountUsd = fromTokenAmount;
            } else if (price > 0) {
                amountUsd = fromTokenAmount * price;
            }
        }

        if (amountUsd > 20 && quantity > 0) {
            totalQuantity += quantity;
            totalAmount += amountUsd;
            swaps.push({
                signature,
                timestamp,
                quantity,
                price: amountUsd / quantity,
                amount: amountUsd
            });
        }
    }

    return {
        [`total${type.charAt(0).toUpperCase() + type.slice(1)}Quantity`]: totalQuantity,
        [`total${type.charAt(0).toUpperCase() + type.slice(1)}Amount`]: totalAmount,
        [`${type}s`]: swaps
    };
}

async function calculateTokenPnl(sellMap, buyMap, solPrice) {
    const tokenPnlMap = new Map();
    const buySellList = [];
    const currentTimestamp = Math.floor(Date.now() / 1000);

    const getLatestTimestamp = (swapList) => {
        if (!swapList || swapList.length === 0) return null;
        return Math.max(...swapList.map(tx => tx.timestamp));
    };

    const getEarliestTimestamp = (swapList) => {
        if (!swapList || swapList.length === 0) return null;
        return Math.min(...swapList.map(tx => tx.timestamp));
    };

    for (const [caMint, sellSwapList] of sellMap.entries()) {
        const sellSummary = summarizeSwaps(sellSwapList, 'sell', solPrice);
        const buySwapList = buyMap.get(caMint);

        const earliestSellTimestamp = getEarliestTimestamp(sellSwapList);
        const latestSellTimestamp = getLatestTimestamp(sellSwapList);

        if (buySwapList && buySwapList.length > 0) {
            buySellList.push(caMint);
            const buySummary = summarizeSwaps(buySwapList, 'buy', solPrice);

            const earliestBuyTimestamp = getEarliestTimestamp(buySwapList);
            const latestBuyTimestamp = getLatestTimestamp(buySwapList);

            let holdingPeriodSeconds = null;
            if (latestSellTimestamp !== null && earliestBuyTimestamp !== null) {
                holdingPeriodSeconds = latestSellTimestamp - earliestBuyTimestamp;
            }

            let latestTransactionTimestamp = null;
            if (latestSellTimestamp !== null && latestBuyTimestamp !== null) {
                latestTransactionTimestamp = Math.max(latestSellTimestamp, latestBuyTimestamp);
            } else if (latestSellTimestamp !== null) {
                latestTransactionTimestamp = latestSellTimestamp;
            } else if (latestBuyTimestamp !== null) {
                latestTransactionTimestamp = latestBuyTimestamp;
            }

            const tokenSymbol = sellSwapList[0]?.fromToken?.symbol || 'Unknown';
            tokenPnlMap.set(caMint, {
                tokenMint: caMint,
                tokenSymbol,
                ...buySummary,
                ...sellSummary,
                holdingPeriodSeconds,
                earliestBuyTimestamp,
                latestBuyTimestamp,
                earliestSellTimestamp,
                latestSellTimestamp,
                latestTransactionTimestamp,
                hasBuys: true,
                hasSells: true
            });
        } else {
            const latestTransactionTimestamp = latestSellTimestamp;
            const tokenSymbol = sellSwapList[0]?.fromToken?.symbol || 'Unknown';
            tokenPnlMap.set(caMint, {
                tokenMint: caMint,
                tokenSymbol,
                ...sellSummary,
                buys: [],
                totalBuyAmount: 0,
                totalBuyQuantity: 0,
                earliestBuyTimestamp: null,
                latestBuyTimestamp: null,
                earliestSellTimestamp,
                latestSellTimestamp,
                latestTransactionTimestamp,
                holdingPeriodSeconds: null,
                hasBuys: false,
                hasSells: true
            });
        }
    }

    for (const [caMint, buySwapList] of buyMap.entries()) {
        if (!buySellList.includes(caMint)) {
            const buySummary = summarizeSwaps(buySwapList, 'buy', solPrice);

            const earliestBuyTimestamp = getEarliestTimestamp(buySwapList);
            const latestBuyTimestamp = getLatestTimestamp(buySwapList);

            const latestTransactionTimestamp = latestBuyTimestamp;

            let holdingPeriodSeconds = null;
            if (earliestBuyTimestamp !== null) {
                holdingPeriodSeconds = currentTimestamp - earliestBuyTimestamp;
            }

            const tokenSymbol = buySwapList[0]?.toToken?.symbol || 'Unknown';
            tokenPnlMap.set(caMint, {
                tokenMint: caMint,
                tokenSymbol,
                ...buySummary,
                sells: [],
                totalSellAmount: 0,
                totalSellQuantity: 0,
                earliestBuyTimestamp,
                latestBuyTimestamp,
                earliestSellTimestamp: null,
                latestSellTimestamp: null,
                latestTransactionTimestamp,
                holdingPeriodSeconds,
                hasBuys: true,
                hasSells: false
            });
        }
    }

    return tokenPnlMap;
}

function calculateProfitSummary(tokenPnlMap) {
    return Array.from(tokenPnlMap.values())
        .filter(tokenPnl => tokenPnl.buys.length > 0 || tokenPnl.sells.length > 0)
        .map(tokenPnl => {
            const profit = tokenPnl.totalSellAmount - tokenPnl.totalBuyAmount;
            const profitRatio = tokenPnl.totalBuyAmount > 0
                ? (profit / tokenPnl.totalBuyAmount) * 100
                : (tokenPnl.totalSellAmount > 0 ? Infinity : 0);

            return {
                tokenMint: tokenPnl.tokenMint,
                tokenSymbol: tokenPnl.tokenSymbol,
                totalBuyAmount: tokenPnl.totalBuyAmount,
                totalSellAmount: tokenPnl.totalSellAmount,
                totalBuyQuantity: tokenPnl.totalBuyQuantity,
                totalSellQuantity: tokenPnl.totalSellQuantity,
                profit,
                profitRatio,
                buyCount: tokenPnl.buys.length,
                sellCount: tokenPnl.sells.length,
                holdingTime: tokenPnl.holdingPeriodSeconds
            };
        })
        .sort((a, b) => {
            const timeA = a.latestTransactionTimestamp || 0;
            const timeB = b.latestTransactionTimestamp || 0;
            return timeB - timeA;
        });
}

// ===================== 条件检查函数 =====================
function checkTokensFilterCondition(tokens, minPnlUsd = 10000) {
    if (!tokens || tokens.length === 0) {
        return false;
    }

    const tokenCount = tokens.length;
    let sumPnlUsd = 0;
    let unHoldingCount = 0;
    let meetsCondition = false;
    let midHoldingCount = 0;

    for (const tokenPnl of tokens) {
        const profitRatio = tokenPnl.profitRatio || 0;
        const holdingTime = tokenPnl.holdingTime;

        if (holdingTime != null && holdingTime < 3 * 60) {
            unHoldingCount += 1;
        }

        if (profitRatio > 200) {
            sumPnlUsd += tokenPnl.profit;
            meetsCondition = true;
        }

        if (tokenCount > 10 && holdingTime > 60 * 60 * 12) {
            midHoldingCount += 1;
            meetsCondition = true;
        }
    }

    const unHoldingRate = unHoldingCount / tokenCount;
    if (unHoldingRate > 0.5) {
        return false;
    }

    const midHoldingRate = midHoldingCount / tokenCount;
    if (tokenCount > 10 && midHoldingRate < 0.5) {
        return false;
    }

    if (meetsCondition && sumPnlUsd > minPnlUsd) {
        return true;
    }

    return false;
}

// ===================== 核心业务逻辑函数 =====================
async function getWalletPnlDetailsCore(walletAddress, paginationToken = null, isFirstQuery) {
    try {
        const apiKey = process.env.HELIUS_API_KEY || config?.helius?.apiKey;

        if (!walletAddress) {
            throw new Error('钱包地址不能为空');
        }
        if (!apiKey) {
            throw new Error('Helius API密钥未配置');
        }

        const { transactions: currentPageTransactions, nextPaginationToken } = await fetchSwapTransactionsByHeliusSwapType(
            walletAddress,
            apiKey,
            paginationToken,
            isFirstQuery
        );

        const parsedSwaps = await parseAllSwaps(currentPageTransactions, walletAddress);
        //const tokenMetadataMap = await fetchTokenMetadata(parsedSwaps, apiKey);
        const tokenMetadataMap = new Map();
        const solPrice = await getSolUsdPrice();
        const { sellMap, buyMap } = groupTransactionsByType(parsedSwaps, tokenMetadataMap, solPrice);
        const tokenPnlMap = await calculateTokenPnl(sellMap, buyMap, solPrice);
        const tokenPnlSummary = calculateProfitSummary(tokenPnlMap);

        return {
            success: true,
            data: {
                walletAddress,
                transactionCount: currentPageTransactions.length,
                swapCount: parsedSwaps.length,
                tokens: tokenPnlSummary,
                pagination: {
                    currentToken: paginationToken,
                    nextToken: nextPaginationToken,
                    hasMore: nextPaginationToken !== null
                },
                timestamp: new Date().toISOString()
            }
        };
    } catch (error) {
        console.error('💥 获取钱包盈亏详情失败:', error.message);
        return {
            success: false,
            error: error.message,
            message: '获取数据失败，请稍后重试',
            timestamp: new Date().toISOString()
        };
    }
}

// ===================== HTTP处理器函数 =====================
export async function syncAnalyzeHolderPnlByHeliusSwapTypeHandler(req, res) {
    try {
        if (!config.birdeye.apiKey) {
            return res.status(400).json({
                success: false,
                error: '未配置 Birdeye API Key',
                hint: '请在 .env 文件中设置 BIRDEYE_API_KEY'
            });
        }

        const { getGroupedTokenAddresses } = await import('../db/solTradeInfoMapper.js');
        const minAmount = parseInt(req.query.minAmount) || 1500000;
        const records = await getGroupedTokenAddresses(minAmount);

        // const records = ["DoYpxQFyzrfNHf64E1mTyUdNhdkiTJJtpYaccLp9Kj3g", "37vV8kjrjQ2XcUxG2nKNpTGQNpfUK6k1LEfAuzSH6JXJ"]

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

        let processedCount = 0;
        let skippedCount = 0;
        const minPnlUsd = parseInt(req.query.minPnlUsd) || 5000;
        const MAX_PAGES = 50; // 最大页数限制

        const walletAddressList = records.map(a => a.addr);
        const zhiShouAddrsResult = await getZhiShouAddr(walletAddressList);
        const zhiShouAddrs =  zhiShouAddrsResult.map(item => item.addr)

        let notExistInZhiShouAddrs = walletAddressList;
        if (zhiShouAddrs != null && zhiShouAddrs.length > 0) {
            notExistInZhiShouAddrs = walletAddressList.filter(addr => !zhiShouAddrs.includes(addr));
        }


        for (const walletAddress of notExistInZhiShouAddrs) {

            let paginationToken = null;
            let firstPage = 1;
            const addrPnlList = [];

            // 数据库已经存在不查询
            const existed = await checkPnlInfoExist(walletAddress);
            if (existed) {
                console.log(`查询到 ${walletAddress} 的pnl记录`);
                skippedCount++;
                continue;
            }

            processedCount++;

            while (true) {
                console.log(`获取地址 ${walletAddress} 的第 ${firstPage} 页数据...`);
                const pnlData = await getWalletPnlDetailsDirect(walletAddress, paginationToken, firstPage);

                if (!pnlData.success) {
                    console.warn(`地址:${walletAddress} 获取钱包盈利返回异常`);
                    break;
                }

                // 检查是否有数据
                if (!pnlData.data || !pnlData.data.tokens) {
                    console.warn(`地址:${walletAddress} 返回的数据格式不正确`);
                    break;
                }

                const filterPnlData = pnlData.data.tokens.filter(da => da.totalBuyAmount > 300);

                if (firstPage === 1) {
                    // 纸手类返回 false
                    const skippedFlag = checkTokensFilterCondition(pnlData.data.tokens, minPnlUsd);
                    if (!skippedFlag) {
                        console.info(`地址:${walletAddress} 不满足条件,被过滤`);
                        await saveAndMarkAddr(walletAddress);
                        break;
                    } else {
                        if (filterPnlData.length > 0) {
                            addrPnlList.push(...filterPnlData);
                        }
                    }
                } else {
                    // 非第一页
                    if (filterPnlData != null && filterPnlData.length > 0) {
                        addrPnlList.push(...filterPnlData);
                    }
                }

                // 检查是否还有下一页 - 关键修复！
                if (!pnlData.data.pagination || pnlData.data.pagination.nextToken == null) {
                    console.info(`地址:${walletAddress} 没有下页数据，查询结束`);
                    break;
                }

                // 更新分页token
                paginationToken = pnlData.data.pagination.nextToken;
                firstPage += 1;

                console.info(`地址:${walletAddress} 满足条件，第:${firstPage}页`);

                // 检查最大页数限制
                if (firstPage > MAX_PAGES) {
                    console.info(`地址:${walletAddress} 达到最大页数限制 ${MAX_PAGES}，停止查询`);
                    break;
                }
            }

            if (addrPnlList && addrPnlList.length > 0) {
                await batchSavePnlInfo(addrPnlList, walletAddress);
            }
        }

        return res.json({
            success: true,
            message: '分析完成',
            totalRecords: records.length,
            processedCount,
            skippedCount
        });
    } catch (error) {
        console.error('同步分析持有者盈亏失败:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
}


export async function syncAnalyzeHolderPnlHandler(req, res) {
    try {
        if (!config.birdeye.apiKey) {
            return res.status(400).json({
                success: false,
                error: '未配置 Birdeye API Key',
                hint: '请在 .env 文件中设置 BIRDEYE_API_KEY'
            });
        }

        const { getGroupedTokenAddresses } = await import('../db/solTradeInfoMapper.js');
        const minAmount = parseInt(req.query.minAmount) || 1500000;
        const records = ["FTg1gqW7vPm4kdU1LPM7JJnizbgPdRDy2PitKw6mY27j"];
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

        let processedCount = 0;
        let savedCount = 0;
        let skippedCount = 0;
        const minPnlUsd = parseInt(req.query.minPnlUsd) || 5000;

        for (const record of records) {
            const walletAddress = record;
            let paginationToken = null;
            let firstPage = 1;
            const addrPnlList = [];

            while (true) {
                const pnlData = await getWalletPnlDetailsDirect(walletAddress, paginationToken, firstPage);

                if (!pnlData.success) {
                    console.warn(`地址:${walletAddress} 获取钱包盈利返回异常`);
                    break;
                }

                if (firstPage === 1 && pnlData.data.pagination.nextToken == null) {
                    console.info(`地址:${walletAddress} 没有下页数据`);
                    break;
                }

                if (firstPage === 1) {
                    const skippedFlag = checkTokensFilterCondition(pnlData.data.tokens, minPnlUsd);
                    if (!skippedFlag) {
                        console.info(`地址:${walletAddress} 不满足条件,被过滤`);
                        break;
                    }
                }

                paginationToken = pnlData.data.pagination.nextToken;
                firstPage += 1;

                const filterPnlData = pnlData.data.tokens.filter(da => da.totalBuyAmount > 300);
                console.info(`地址:${walletAddress} 满足条件，第:${firstPage}页`);
                if (filterPnlData != null) {
                    addrPnlList.push(...filterPnlData);
                }
            }

            if (addrPnlList && addrPnlList.length > 0) {
                await batchSavePnlInfo(addrPnlList, walletAddress);
            }
        }

        return res.json({
            success: true,
            message: '分析完成',
            totalRecords: records.length,
            processedCount,
            savedCount,
            skippedCount
        });
    } catch (error) {
        console.error('同步分析持有者盈亏失败:', error);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
}

export async function getWalletPnlDetailsByHeliusTypeHandler(req, res) {
    const { walletAddress } = req.params;

    if (!walletAddress || walletAddress.trim() === '') {
        return res.status(400).json({
            success: false,
            error: '钱包地址不能为空'
        });
    }

    if (!config?.helius?.apiKey) {
        return res.status(500).json({
            success: false,
            error: '没有配置helius api key'
        });
    }

    const apiKey = config.helius.apiKey;
    let allTransactions = [];
    let before = null;
    let pageCount = 0;
    const limit = 100;

    while (true) {
        pageCount++;
        console.log(`📄 正在查询第 ${pageCount} 页...`);

        const urlParams = new URLSearchParams();
        urlParams.append('type', 'SWAP');
        urlParams.append('limit', limit.toString());
        if (before != null) {
            urlParams.append("before", before);
        }

        const queryString = urlParams.toString();
        const apiUrl = `https://api.helius.xyz/v0/addresses/${walletAddress}/transactions?api-key=${apiKey}&${queryString}`;

        const response = await fetch(apiUrl, {
            method: 'GET',
            headers: {
                'Accept': 'application/json',
                'Content-Type': 'application/json'
            }
        });

        if (!response.ok) {
            const errorText = await response.text();
            console.error(`❌ Helius API 错误:`, {
                status: response.status,
                error: errorText
            });
            break;
        }

        let transactions = await response.json();

        if (transactions != null && transactions.length > 0) {
            transactions.sort((a, b) => b.timestamp - a.timestamp);
            allTransactions.push(...transactions);
            before = transactions[transactions.length - 1].signature;
        }

        if (transactions.length === 0) {
            break;
        }

        if (before == null) {
            console.log(`⚠ before 为null,停止查询`);
            break;
        }

        await sleep(200);
    }

    return res.json({
        success: true,
        transactions: allTransactions
    });
}

export async function getWalletPnlDetailsByHeliusHandler(req, res) {
    try {
        const { walletAddress } = req.params;
        const { paginationToken } = req.query;

        const result = await getWalletPnlDetailsCore(walletAddress, paginationToken || null);

        if (result.success) {
            return res.json(result);
        } else {
            let statusCode = 500;
            if (result.error?.includes('钱包地址不能为空')) {
                statusCode = 400;
            } else if (result.error?.includes('API密钥未配置')) {
                statusCode = 500;
            }

            return res.status(statusCode).json({
                success: false,
                error: result.error,
                message: process.env.NODE_ENV === 'development' ? result.error : '请稍后重试',
                requestId: `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`,
                timestamp: result.timestamp
            });
        }
    } catch (error) {
        console.error('💥 处理请求失败:', {
            message: error.message,
            stack: error.stack,
            timestamp: new Date().toISOString()
        });

        return res.status(500).json({
            success: false,
            error: error.message,
            message: process.env.NODE_ENV === 'development' ? error.message : '请稍后重试',
            requestId: `req_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
        });
    }
}

export async function getWalletPnlDetailsDirect(walletAddress, paginationToken = null, firstPage) {
    return await getWalletPnlDetailsCore(walletAddress, paginationToken, firstPage);
}