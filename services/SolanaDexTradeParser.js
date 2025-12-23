import { Connection, PublicKey, clusterApiUrl } from '@solana/web3.js';

/**
 * 简单的内存缓存
 */
class SimpleCache {
    constructor(maxSize = 1000, ttl = 3600000) { // 默认 1 小时过期
        this.cache = new Map();
        this.maxSize = maxSize;
        this.ttl = ttl;
    }

    get(key) {
        const item = this.cache.get(key);
        if (!item) return null;
        
        // 检查是否过期
        if (Date.now() - item.timestamp > this.ttl) {
            this.cache.delete(key);
            return null;
        }
        
        return item.value;
    }

    set(key, value) {
        // 如果缓存已满，删除最旧的项
        if (this.cache.size >= this.maxSize) {
            const firstKey = this.cache.keys().next().value;
            this.cache.delete(firstKey);
        }
        
        this.cache.set(key, {
            value,
            timestamp: Date.now()
        });
    }

    clear() {
        this.cache.clear();
    }
}

/**
 * 完整的 Solana DEX 交易解析器
 * 支持: Jupiter, Raydium, Orca, Pump.fun AMM
 */
class SolanaDexTradeParser {
    constructor(connection = null) {
        this.connection = connection || new Connection(clusterApiUrl('mainnet-beta'));
        // 添加缓存
        this.cache = new SimpleCache(500, 3600000); // 缓存 500 个交易，1 小时过期

        // 定义所有 DEX 的程序 ID
        this.DEX_PROGRAMS = {
            JUPITER: [
                'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', // V3
                'JUP2jxvXaqu7NQY1GmNF4m1vodw12LVXYxbFL2uJvfo', // V4
                'JUP3c2Uh3WA4Ng34tw6kPd2G4C5BB21Xo36Je1s32Ph', // V5
                'JUP6i4ozu5ydDCnLiMogSckDPpbtr7BJ4FtzYWkb5Rk',  // V6
                'JUP4Fb2cqiRUcaTd8t5VhYu6oV5E2hbN8FdY3YbwPEsu', // V4 (alternative)
                'JUP3c2Uh3WA4Ng34tw6kPd2G4C5BB21Xo36Je1s32Ph'  // V5 (duplicate check)
            ],
            RAYDIUM: [
                '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // V4
                '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv', // V3
                'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK'  // AMM V4 / CLMM
            ],
            ORCA: [
                '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', // 主程序
                'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc',  // Whirlpools
                'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1'  // Swap V1
            ],
            PUMP_FUN: ['pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA'], // Pump.fun AMM
            METEORA: [
                'LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo', // Meteora DLMM
                'Eo7WjKq67rjJQSZxS6z3YkapzY3eMj6Xy8X5EQVn5UWAi', // Meteora DLMM (alternative)
                '24Uqj9JQxErmqU6wvJzKqJqJqJqJqJqJqJqJqJqJqJqJq'  // Placeholder - will be updated with actual IDs
            ],
            DFLOW: [
                // DFlow Aggregator v4 program IDs (will be identified via log messages)
            ]
        };

        // SOL 代币地址
        this.SOL_MINT = 'So11111111111111111111111111111111111111112';
    }

    /**
     * 主解析函数 - 解析交易签名并返回买卖信息
     * @param {string} signature - 交易签名
     * @param {object} options - 选项
     * @returns {Promise<object|null>} 交易信息或null
     */
    async parseTrade(signature, options = {}) {
        const {
            commitment = 'confirmed',
            maxSupportedTransactionVersion = 0,
            useJupiterAPI = true,
            useCache = true
        } = options;

        // 检查缓存
        if (useCache) {
            const cached = this.cache.get(signature);
            if (cached) {
                return cached;
            }
        }

        try {
            // 1. 并行尝试 Jupiter API 和获取交易数据（如果启用 Jupiter API）
            let jupiterTrade = null;
            let transaction = null;

            const promises = [];
            
            if (useJupiterAPI) {
                promises.push(
                    this.parseJupiterViaAPI(signature).catch(() => null)
                );
            }
            
            promises.push(
                this.connection.getParsedTransaction(signature, {
                    commitment,
                    maxSupportedTransactionVersion
                }).catch(err => {
                    console.error(`Error fetching transaction ${signature}:`, err.message);
                    return null;
                })
            );

            const results = await Promise.all(promises);
            
            if (useJupiterAPI) {
                jupiterTrade = results[0];
                transaction = results[1];
            } else {
                transaction = results[0];
            }

            // 如果 Jupiter API 返回了结果，先检查是否是 Pump.fun 交易
            // 如果是 Pump.fun 交易，优先使用 Pump.fun 解析器（更准确）
            if (jupiterTrade && transaction) {
                const dexType = this.identifyDEX(transaction);
                
                // 如果是 Pump.fun 交易，使用 Pump.fun 解析器覆盖 Jupiter API 的结果
                if (dexType === 'pump_fun') {
                    const pumpFunTrade = this.parsePumpFunTransaction(transaction);
                    if (pumpFunTrade) {
                        // Pump.fun 解析器返回的结果更准确，使用它
                        if (useCache) {
                            this.cache.set(signature, pumpFunTrade);
                        }
                        return pumpFunTrade;
                    }
                }
                
                // 对于其他交易，使用 Jupiter API 的结果，但需要补充持有人地址
                if (!jupiterTrade.holderAddress) {
                    // 优先从 accountKeys 中获取第一个签名者
                    const accountKeys = transaction.transaction?.message?.accountKeys || [];
                    const signerAccount = accountKeys.find(acc => {
                        if (typeof acc === 'object' && acc.signer) {
                            return true;
                        }
                        return false;
                    });
                    
                    if (signerAccount) {
                        jupiterTrade.holderAddress = typeof signerAccount === 'string' 
                            ? signerAccount 
                            : (signerAccount.pubkey?.toString() || signerAccount.toString?.() || String(signerAccount));
                    } else {
                        // 如果没有找到签名者，从 SOL 余额变化中找签名者
                        const solChanges = this.parseSOLBalanceChanges(transaction);
                        const signerChange = solChanges.find(change => change.isSigner);
                        if (signerChange) {
                            jupiterTrade.holderAddress = signerChange.account;
                        } else if (solChanges.length > 0) {
                            jupiterTrade.holderAddress = solChanges[0].account;
                        }
                    }
                }
                if (useCache) {
                    this.cache.set(signature, jupiterTrade);
                }
                return jupiterTrade;
            }

            if (!transaction) {
                throw new Error('Transaction not found on chain');
            }

            // 2. 识别 DEX 类型并解析
            const dexType = this.identifyDEX(transaction);

            let tradeInfo;
            switch (dexType) {
                case 'jupiter':
                    tradeInfo = this.parseJupiterTransaction(transaction);
                    break;
                case 'raydium':
                    tradeInfo = this.parseRaydiumTransaction(transaction);
                    break;
                case 'orca':
                    tradeInfo = this.parseOrcaTransaction(transaction);
                    break;
                case 'pump_fun':
                    tradeInfo = this.parsePumpFunTransaction(transaction);
                    break;
                default:
                    // 对于 unknown 类型，也尝试使用通用解析器
                    // 通用解析器会分析代币和 SOL 余额变化，应该能处理大部分情况
                    tradeInfo = this.parseGenericTransaction(transaction, 'unknown');
            }

            if (tradeInfo) {
                tradeInfo.signature = signature;
                tradeInfo.timestamp = transaction.blockTime ? new Date(transaction.blockTime * 1000) : null;
                tradeInfo.slot = transaction.slot;
            }

            // 缓存结果
            if (useCache && tradeInfo) {
                this.cache.set(signature, tradeInfo);
            }

            return tradeInfo;

        } catch (error) {
            console.error(`Error parsing trade ${signature}:`, error.message);
            return null;
        }
    }

    /**
     * 识别交易来自哪个 DEX
     * @param {object} transaction - 解析后的交易数据
     * @returns {string} DEX 类型
     */
    identifyDEX(transaction) {
        // 辅助函数：从指令中获取 programId
        const getProgramId = (ix, accountKeys) => {
            // 处理 parsed 和 unparsed 指令
            if (ix.programId) {
                return typeof ix.programId === 'string' ? ix.programId : ix.programId.toString();
            }
            // 如果是 unparsed 指令，从 accountKeys 中获取
            if (typeof ix.programIdIndex === 'number' && accountKeys) {
                const account = accountKeys[ix.programIdIndex];
                if (account) {
                    // 安全地获取 account 地址
                    if (typeof account === 'string') {
                        return account;
                    }
                    // 如果是对象，尝试获取 pubkey
                    if (account && typeof account === 'object') {
                        return account.pubkey?.toString() || account.toString?.() || String(account);
                    }
                    return String(account);
                }
            }
            return null;
        };

        const accountKeys = transaction.transaction.message.accountKeys;
        
        // 收集主指令的 programIds
        const mainProgramIds = transaction.transaction.message.instructions
            .map(ix => getProgramId(ix, accountKeys))
            .filter(id => id !== null);
        
        // 收集 innerInstructions 的 programIds
        const innerProgramIds = [];
        const innerInstructions = transaction.meta?.innerInstructions || [];
        innerInstructions.forEach(inner => {
            if (Array.isArray(inner.instructions)) {
                inner.instructions.forEach(ix => {
                    const programId = getProgramId(ix, accountKeys);
                    if (programId) {
                        innerProgramIds.push(programId);
                    }
                });
            }
        });
        
        // 合并所有 programIds（优先使用 innerInstructions，因为它们代表实际执行的 DEX）
        const programIds = [...innerProgramIds, ...mainProgramIds];

        // 检查所有已知的 DEX 程序 ID
        // 优先检查 Pump.fun（即使通过 Jupiter 路由，实际执行交易的 DEX 更重要）
        const priorityDEXs = ['PUMP_FUN', 'RAYDIUM', 'ORCA', 'JUPITER', 'METEORA', 'DFLOW'];
        for (const dex of priorityDEXs) {
            const programs = this.DEX_PROGRAMS[dex];
            if (programs && programs.some(program => programIds.includes(program))) {
                return dex.toLowerCase();
            }
        }
        
        // 如果优先级列表中没有匹配，检查其他 DEX
        for (const [dex, programs] of Object.entries(this.DEX_PROGRAMS)) {
            if (!priorityDEXs.includes(dex) && programs.some(program => programIds.includes(program))) {
                return dex.toLowerCase();
            }
        }

        // 通过日志消息识别 DEX（用于识别 Meteora DLMM、DFlow 和 Pump.fun 等）
        const logMessages = transaction.meta?.logMessages || [];
        const logText = logMessages.join(' ').toLowerCase();
        
        // 识别 Pump.fun（通过日志中的程序 ID 或指令类型）
        const PUMP_FUN_PROGRAM_ID = this.DEX_PROGRAMS.PUMP_FUN[0];
        if (logText.includes(PUMP_FUN_PROGRAM_ID.toLowerCase()) || 
            (logText.includes('instruction: sell') && logText.includes('pamm'))) {
            return 'pump_fun';
        }
        
        // 识别 Meteora DLMM
        if (logText.includes('meteora') || logText.includes('dlmm')) {
            return 'meteora';
        }
        
        // 识别 DFlow Aggregator
        if (logText.includes('dflow') || logText.includes('aggregator')) {
            return 'dflow';
        }

        // 如果没有找到已知的 DEX，尝试通过其他特征识别
        // 检查是否可能是 Jupiter 交易（通过账户数量、指令复杂度等特征）
        // Jupiter 交易通常涉及多个账户和复杂的路由
        const instructionCount = transaction.transaction.message.instructions.length;
        const accountCount = transaction.transaction.message.accountKeys.length;
        
        // Jupiter 交易通常有多个指令和账户
        if (instructionCount > 3 && accountCount > 10) {
            // 检查是否有典型的 DEX 交易特征（代币余额变化 + SOL 余额变化）
            const hasTokenChanges = transaction.meta?.postTokenBalances?.length > 0;
            const hasSolChanges = transaction.meta?.postBalances?.some((post, i) => {
                const pre = transaction.meta?.preBalances?.[i] || 0;
                return Math.abs(post - pre) > 1000000; // 大于 0.001 SOL
            });

            if (hasTokenChanges && hasSolChanges) {
                // 很可能是 Jupiter 或其他聚合器交易
                // 只在开发环境记录调试信息
                if (process.env.NODE_ENV === 'development') {
                    console.log(`[DEX识别] 通过特征识别为 Jupiter 交易: ${instructionCount} 指令, ${accountCount} 账户`);
                }
                return 'jupiter'; // 假设是 Jupiter，使用通用解析器处理
            }
        }

        // 只在开发环境记录未识别的交易信息
        if (process.env.NODE_ENV === 'development') {
            console.log(`[DEX识别] 未识别的交易类型: ${instructionCount} 指令, ${accountCount} 账户`);
        }
        return 'unknown';
    }

    /**
     * 通过 Jupiter API 解析交易
     * @param {string} signature - 交易签名
     * @returns {Promise<object|null>} 交易信息
     */
    async parseJupiterViaAPI(signature) {
        try {
            // 添加超时控制（2秒，减少等待时间）
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 2000);

            try {
                const response = await fetch(`https://api.jup.ag/transactions/${signature}`, {
                    signal: controller.signal
                });
                clearTimeout(timeoutId);

                if (!response.ok) {
                    // 静默处理 404 等错误，不打印日志
                    return null;
                }

                const data = await response.json();
                if (!data?.inputMint || !data?.outputMint) return null;

                // 计算实际金额
                const inputAmount = data.inputAmount / Math.pow(10, data.inputDecimals || 6);
                const outputAmount = data.outputAmount / Math.pow(10, data.outputDecimals || 6);

                // 验证数据是否合理（金额不能为0或过小）
                if (inputAmount <= 0 || outputAmount <= 0) {
                    return null;
                }

                // 如果金额过小（可能是部分交易或错误数据），也返回 null，让系统使用交易数据本身来解析
                // 例如，如果输入金额小于 0.01 SOL，可能是部分交易数据
                if (data.inputMint === this.SOL_MINT && inputAmount < 0.01) {
                    return null;
                }

                // 注意：Jupiter API 不直接提供持有人地址，需要在调用后从交易数据中获取
                return {
                    type: this.inferTradeType(data.inputMint, data.outputMint),
                    soldToken: {
                        mint: data.inputMint,
                        symbol: data.inputSymbol || 'Unknown',
                        amount: inputAmount,
                        decimals: data.inputDecimals || 6
                    },
                    boughtToken: {
                        mint: data.outputMint,
                        symbol: data.outputSymbol || 'Unknown',
                        amount: outputAmount,
                        decimals: data.outputDecimals || 6
                    },
                    price: data.price,
                    fee: data.fee,
                    route: data.routePlan,
                    dex: 'jupiter',
                    source: 'jupiter_api',
                    holderAddress: null // Jupiter API 不提供，需要从交易数据中获取
                };
            } catch (fetchError) {
                clearTimeout(timeoutId);
                // 如果是超时或网络错误，静默返回 null，让系统使用交易数据本身来解析
                if (fetchError.name === 'AbortError' || fetchError.code === 'UND_ERR_CONNECT_TIMEOUT') {
                    return null;
                }
                throw fetchError;
            }
        } catch (error) {
            // 静默处理错误，让系统回退到使用交易数据本身来解析
            // 不打印错误日志，避免噪音
            return null;
        }
    }

    /**
     * 解析 Jupiter 交易
     * @param {object} transaction - 交易数据
     * @returns {object|null} 交易信息
     */
    parseJupiterTransaction(transaction) {
        // Jupiter 交易通常很复杂，我们使用通用解析
        const tradeInfo = this.parseGenericTransaction(transaction, 'jupiter');
        if (tradeInfo) {
            tradeInfo.source = 'jupiter_transaction';
            // 只在开发环境添加调试信息
            if (process.env.NODE_ENV === 'development') {
                const tokenChanges = this.parseTokenBalanceChanges(transaction.meta);
                const solChanges = this.parseSOLBalanceChanges(transaction);
                tradeInfo._debug = {
                    totalSolChanges: solChanges.length,
                    totalTokenChanges: tokenChanges.length,
                    solChangesSummary: solChanges.slice(0, 5).map(c => ({
                        account: c.account.substring(0, 8) + '...',
                        change: c.change,
                        isSigner: c.isSigner
                    })),
                    tokenChangesSummary: tokenChanges.slice(0, 5).map(c => ({
                        mint: c.mint.substring(0, 8) + '...',
                        change: c.change
                    }))
                };
            }
        }
        return tradeInfo;
    }

    /**
     * 解析 Raydium 交易
     * @param {object} transaction - 交易数据
     * @returns {object|null} 交易信息
     */
    parseRaydiumTransaction(transaction) {
        const { meta, transaction: txData } = transaction;

        // 查找 Raydium swap 指令
        const swapInstruction = txData.message.instructions.find(ix =>
            ix.programId?.toString() === this.DEX_PROGRAMS.RAYDIUM[0] &&
            ix.parsed?.type === 'swap'
        );

        // 分析代币余额变化
        const tradeInfo = this.analyzeSOLTokenTrade(
            this.parseTokenBalanceChanges(meta),
            this.parseSOLBalanceChanges(transaction)
        );

        if (tradeInfo) {
            return {
                ...tradeInfo,
                dex: 'raydium',
                source: 'raydium_parser',
                hasSwapInstruction: !!swapInstruction
            };
        }

        return null;
    }

    /**
     * 解析 Orca 交易
     * @param {object} transaction - 交易数据
     * @returns {object|null} 交易信息
     */
    parseOrcaTransaction(transaction) {
        const { meta, transaction: txData } = transaction;

        // 查找 Orca 相关指令
        const orcaInstructions = txData.message.instructions.filter(ix =>
            this.DEX_PROGRAMS.ORCA.includes(ix.programId?.toString())
        );

        // 分析代币余额变化
        const tradeInfo = this.analyzeSOLTokenTrade(
            this.parseTokenBalanceChanges(meta),
            this.parseSOLBalanceChanges(transaction)
        );

        if (tradeInfo) {
            return {
                ...tradeInfo,
                dex: 'orca',
                source: 'orca_parser',
                orcaInstructions: orcaInstructions.length
            };
        }

        return null;
    }

    /**
     * 解析 Pump.fun AMM 交易
     * @param {object} transaction - 交易数据
     * @returns {object|null} 交易信息
     */
    parsePumpFunTransaction(transaction) {
        const { meta, transaction: txData } = transaction;
        const PUMP_FUN_PROGRAM_ID = this.DEX_PROGRAMS.PUMP_FUN[0];

        // 辅助函数：从指令中获取 programId
        const getProgramId = (ix, accountKeys) => {
            // 如果是 parsed 格式，直接使用 programId
            if (ix.programId) {
                return typeof ix.programId === 'string' ? ix.programId : ix.programId.toString();
            }
            // 如果是 unparsed 格式，从 accountKeys 中获取
            if (typeof ix.programIdIndex === 'number' && accountKeys) {
                const account = accountKeys[ix.programIdIndex];
                if (account) {
                    if (typeof account === 'string') {
                        return account;
                    }
                    if (account && typeof account === 'object') {
                        return account.pubkey?.toString() || account.toString?.() || String(account);
                    }
                    return String(account);
                }
            }
            return null;
        };

        const accountKeys = txData.message.accountKeys;
        
        // 查找主指令中的 Pump.fun 相关指令
        const mainPumpFunInstructions = txData.message.instructions.filter(ix => {
            const programId = getProgramId(ix, accountKeys);
            return programId === PUMP_FUN_PROGRAM_ID;
        });
        
        // 查找 innerInstructions 中的 Pump.fun 相关指令（重要：通过路由器调用的 Pump.fun 交易）
        const innerPumpFunInstructions = [];
        const innerInstructions = meta?.innerInstructions || [];
        innerInstructions.forEach(inner => {
            if (Array.isArray(inner.instructions)) {
                inner.instructions.forEach(ix => {
                    const programId = getProgramId(ix, accountKeys);
                    if (programId === PUMP_FUN_PROGRAM_ID) {
                        innerPumpFunInstructions.push(ix);
                    }
                });
            }
        });

        // 检查日志消息以确定交易方向（Pump.fun 日志中会明确指示 Buy 或 Sell）
        const logMessages = meta?.logMessages || [];
        const logText = logMessages.join(' ').toLowerCase();
        
        // 如果主指令和 innerInstructions 中都没有 Pump.fun 指令，但日志中包含 Pump.fun 程序 ID，仍然继续解析
        // 这是因为某些交易可能通过路由器调用，程序 ID 不在 accountKeys 中
        const PUMP_FUN_PROGRAM_ID_LOWER = PUMP_FUN_PROGRAM_ID.toLowerCase();
        const hasPumpFunInLogs = logText.includes(PUMP_FUN_PROGRAM_ID_LOWER);
        
        if (mainPumpFunInstructions.length === 0 && innerPumpFunInstructions.length === 0 && !hasPumpFunInLogs) {
            return null;
        }
        
        const isSellFromLog = logText.includes('instruction: sell') || logText.includes('instruction sell');
        const isBuyFromLog = logText.includes('instruction: buy') || logText.includes('instruction buy');

        // Pump.fun 交易通常是 SOL ↔ Meme Token
        const tokenChanges = this.parseTokenBalanceChanges(meta);
        const solChanges = this.parseSOLBalanceChanges(transaction);

        const tradeInfo = this.analyzeSOLTokenTrade(tokenChanges, solChanges);

        if (tradeInfo) {
            // 如果日志明确指示了方向，且与解析结果不一致，使用日志的方向
            if (isSellFromLog && tradeInfo.type === 'buy') {
                // 交换 soldToken 和 boughtToken
                const temp = tradeInfo.soldToken;
                tradeInfo.soldToken = tradeInfo.boughtToken;
                tradeInfo.boughtToken = temp;
                tradeInfo.type = 'sell';
                tradeInfo.price = tradeInfo.boughtToken.amount / tradeInfo.soldToken.amount;
            } else if (isBuyFromLog && tradeInfo.type === 'sell') {
                // 交换 soldToken 和 boughtToken
                const temp = tradeInfo.soldToken;
                tradeInfo.soldToken = tradeInfo.boughtToken;
                tradeInfo.boughtToken = temp;
                tradeInfo.type = 'buy';
                tradeInfo.price = tradeInfo.boughtToken.amount / tradeInfo.soldToken.amount;
            }
            
            return {
                ...tradeInfo,
                dex: 'pump_fun_amm',
                source: 'pump_fun_parser',
                platform: 'pump.fun',
                instructions: mainPumpFunInstructions.length + innerPumpFunInstructions.length,
                routed: innerPumpFunInstructions.length > 0 // 标记是否通过路由器调用
            };
        }

        return null;
    }

    /**
     * 通用交易解析（后备方案）
     * @param {object} transaction - 交易数据
     * @param {string} dex - DEX 名称
     * @returns {object|null} 交易信息
     */
    parseGenericTransaction(transaction, dex = 'unknown') {
        const { meta, transaction: txData } = transaction;
        
        // 检查是否是流动性操作（如 rebalance_liquidity），这些不是买卖操作
        const instructions = txData?.message?.instructions || [];
        const innerInstructions = meta?.innerInstructions || [];
        const logMessages = meta?.logMessages || [];
        const allLogText = logMessages.join(' ').toLowerCase();
        
        // 检查日志消息中是否包含流动性相关操作
        const liquidityOperations = [
            'rebalance_liquidity',
            'rebalance',
            'add_liquidity',
            'remove_liquidity',
            'deposit',
            'withdraw'
        ];
        
        const hasLiquidityOperation = liquidityOperations.some(op => 
            allLogText.includes(op.toLowerCase())
        );
        
        // 检查指令中是否包含流动性操作
        const hasLiquidityInstruction = instructions.some(ix => {
            if (ix.parsed && typeof ix.parsed === 'object') {
                const instructionType = (ix.parsed.type || '').toLowerCase();
                return liquidityOperations.some(op => instructionType.includes(op.toLowerCase()));
            }
            return false;
        });
        
        // 检查 innerInstructions 中是否包含流动性操作
        const hasLiquidityInnerInstruction = innerInstructions.some(inner => {
            if (Array.isArray(inner.instructions)) {
                return inner.instructions.some(ix => {
                    if (ix.parsed && typeof ix.parsed === 'object') {
                        const instructionType = (ix.parsed.type || '').toLowerCase();
                        return liquidityOperations.some(op => instructionType.includes(op.toLowerCase()));
                    }
                    return false;
                });
            }
            return false;
        });
        
        // 如果是流动性操作，返回 null（不是买卖操作）
        if (hasLiquidityOperation || hasLiquidityInstruction || hasLiquidityInnerInstruction) {
            return null;
        }
        
        const tokenChanges = this.parseTokenBalanceChanges(meta);
        const solChanges = this.parseSOLBalanceChanges(transaction);

        // 获取持有人地址（优先使用签名者）
        const getHolderAddress = () => {
            // 优先从签名者账户获取
            const signerChange = solChanges.find(change => change.isSigner);
            if (signerChange) {
                return signerChange.account;
            }
            // 如果没有签名者，使用第一个发生余额变化的账户
            if (solChanges.length > 0) {
                return solChanges[0].account;
            }
            // 如果都没有，尝试从 accountKeys 中获取第一个签名者
            if (txData?.message?.accountKeys) {
                const signerAccount = txData.message.accountKeys.find(acc => {
                    if (typeof acc === 'object' && acc.signer) {
                        return true;
                    }
                    return false;
                });
                if (signerAccount) {
                    return typeof signerAccount === 'string' 
                        ? signerAccount 
                        : (signerAccount.pubkey?.toString() || signerAccount.toString?.() || String(signerAccount));
                }
            }
            return null;
        };
        
        const holderAddress = getHolderAddress();

        // 先检查是否是代币到代币的交换（通过 SOL 中转）
        // 如果用户账户有多个代币变化（一个减少，一个增加），且 SOL 变化较小，则可能是代币到代币交换
        const significantTokenChanges = tokenChanges.filter(change => {
            if (change.mint === this.SOL_MINT || change.mint === 'SOL') {
                return false;
            }
            return Math.abs(change.change) > 0.000001;
        });
        
        // 找到用户账户的代币变化（优先使用签名者账户）
        const signerSolChanges = solChanges.filter(change => change.isSigner);
        const signerAccounts = new Set(signerSolChanges.map(change => change.account));
        
        // 查找签名者账户的代币变化
        const userTokenChanges = significantTokenChanges.filter(change => 
            signerAccounts.has(change.owner)
        );
        
        // 如果找不到签名者的代币变化，尝试从所有代币变化中找到主要的（按 owner 分组，找到变化最大的）
        let effectiveTokenChanges = userTokenChanges;
        if (effectiveTokenChanges.length === 0) {
            // 按 owner 分组，找到变化最大的账户
            const tokenChangesByOwner = new Map();
            significantTokenChanges.forEach(change => {
                if (!tokenChangesByOwner.has(change.owner)) {
                    tokenChangesByOwner.set(change.owner, []);
                }
                tokenChangesByOwner.get(change.owner).push(change);
            });
            
            // 找到总变化最大的账户（绝对值）
            let maxOwner = null;
            let maxTotalChange = 0;
            tokenChangesByOwner.forEach((changes, owner) => {
                const totalChange = changes.reduce((sum, c) => sum + Math.abs(c.change), 0);
                if (totalChange > maxTotalChange) {
                    maxTotalChange = totalChange;
                    maxOwner = owner;
                }
            });
            
            if (maxOwner) {
                effectiveTokenChanges = tokenChangesByOwner.get(maxOwner);
            }
        }
        
        // 检查是否是代币到代币的交换
        const tokenSold = effectiveTokenChanges.filter(change => change.change < 0);
        const tokenBought = effectiveTokenChanges.filter(change => change.change > 0);
        
        // 如果用户账户有卖出和买入的代币，且都不是 SOL，则认为是代币到代币交换
        if (tokenSold.length > 0 && tokenBought.length > 0) {
            // 找到绝对值最大的卖出和买入
            const maxSold = tokenSold.reduce((max, item) => 
                Math.abs(item.change) > Math.abs(max.change) ? item : max, tokenSold[0]
            );
            const maxBought = tokenBought.reduce((max, item) => 
                Math.abs(item.change) > Math.abs(max.change) ? item : max, tokenBought[0]
            );
            
            // 确认都不是 SOL
            if (maxSold.mint !== this.SOL_MINT && maxBought.mint !== this.SOL_MINT) {
                // 获取用户地址（优先使用签名者）
                const userAddress = effectiveTokenChanges[0]?.owner || holderAddress;
                
                return {
                    type: 'swap',
                    soldToken: {
                        mint: maxSold.mint,
                        symbol: 'Token',
                        amount: Math.abs(maxSold.change),
                        decimals: maxSold.decimals
                    },
                    boughtToken: {
                        mint: maxBought.mint,
                        symbol: 'Token',
                        amount: maxBought.change,
                        decimals: maxBought.decimals
                    },
                    price: maxBought.change / Math.abs(maxSold.change),
                    dex,
                    source: 'generic_parser',
                    note: '代币到代币交换（通过 SOL 中转）',
                    holderAddress: userAddress || holderAddress
                };
            }
        }

        // 首先尝试使用 analyzeSOLTokenTrade（专门处理 SOL 和代币的交易）
        const solTokenTrade = this.analyzeSOLTokenTrade(tokenChanges, solChanges);
        if (solTokenTrade) {
            // 如果 analyzeSOLTokenTrade 没有返回 holderAddress，添加它
            if (!solTokenTrade.holderAddress) {
                solTokenTrade.holderAddress = holderAddress;
            }
            return {
                ...solTokenTrade,
                dex,
                source: 'generic_parser'
            };
        }

        // 如果没有找到 SOL-代币交易，尝试通用解析
        const allChanges = [
            ...tokenChanges.map(change => ({
                type: 'token',
                mint: change.mint,
                amount: change.change,
                decimals: change.decimals
            })),
            ...solChanges.map(change => ({
                type: 'sol',
                mint: this.SOL_MINT,
                amount: change.change,
                decimals: 9
            }))
        ];

        // 过滤掉微小变化
        const significantChanges = allChanges.filter(item => Math.abs(item.amount) > 0.0001);

        const sold = significantChanges.filter(item => item.amount < 0);
        const bought = significantChanges.filter(item => item.amount > 0);

        // 如果只有一个卖出和一个买入，直接返回
        if (sold.length === 1 && bought.length === 1) {
            const type = this.inferTradeType(sold[0].mint, bought[0].mint);

            return {
                type,
                soldToken: {
                    mint: sold[0].mint,
                    symbol: sold[0].mint === this.SOL_MINT ? 'SOL' : 'Token',
                    amount: Math.abs(sold[0].amount),
                    decimals: sold[0].decimals
                },
                boughtToken: {
                    mint: bought[0].mint,
                    symbol: bought[0].mint === this.SOL_MINT ? 'SOL' : 'Token',
                    amount: bought[0].amount,
                    decimals: bought[0].decimals
                },
                price: this.calculatePrice(sold[0], bought[0]),
                dex,
                source: 'generic_parser',
                holderAddress: holderAddress
            };
        }

        // 如果有多个变化，尝试找到主要的交易对（最大的变化）
        if (sold.length > 0 && bought.length > 0) {
            // 找到绝对值最大的卖出和买入
            const maxSold = sold.reduce((max, item) => 
                Math.abs(item.amount) > Math.abs(max.amount) ? item : max
            );
            const maxBought = bought.reduce((max, item) => 
                Math.abs(item.amount) > Math.abs(max.amount) ? item : max
            );

            // 如果主要变化是 SOL 和代币，则认为是有效交易
            const isSOLTokenTrade = 
                (maxSold.mint === 'SOL' && maxBought.type === 'token') ||
                (maxSold.type === 'token' && maxBought.mint === 'SOL');

            if (isSOLTokenTrade) {
                const type = this.inferTradeType(maxSold.mint, maxBought.mint);

                return {
                    type,
                    soldToken: {
                        mint: maxSold.mint,
                        symbol: maxSold.mint === this.SOL_MINT ? 'SOL' : 'Token',
                        amount: Math.abs(maxSold.amount),
                        decimals: maxSold.decimals
                    },
                    boughtToken: {
                        mint: maxBought.mint,
                        symbol: maxBought.mint === this.SOL_MINT ? 'SOL' : 'Token',
                        amount: maxBought.amount,
                        decimals: maxBought.decimals
                    },
                    price: this.calculatePrice(maxSold, maxBought),
                    dex,
                    source: 'generic_parser',
                    note: '交易包含多个变化，已提取主要交易对',
                    holderAddress: holderAddress
                };
            }
        }

        return null;
    }

    /**
     * 分析 SOL 与代币的交易
     * @param {array} tokenChanges - 代币变化
     * @param {array} solChanges - SOL 变化
     * @returns {object|null} 交易信息
     */
    analyzeSOLTokenTrade(tokenChanges, solChanges) {
        
        // 过滤微小变化（降低阈值，避免过滤掉重要变化）
        const significantSolChanges = solChanges.filter(change => Math.abs(change.change) > 0.0001);
        const significantTokenChanges = tokenChanges.filter(change => {
            // 过滤掉 SOL 代币变化（SOL 应该通过 SOL 余额变化处理）
            if (change.mint === this.SOL_MINT || change.mint === 'SOL') {
                return false;
            }
            return Math.abs(change.change) > 0.000001;
        });

        // 找到花费 SOL 的账户（通常是用户账户/签名者账户）
        const solSpentChanges = significantSolChanges.filter(change => change.change < 0);
        const solSpentAccounts = new Set(solSpentChanges.map(change => change.account));
        
        // 找到收到 SOL 的账户
        const solReceivedChanges = significantSolChanges.filter(change => change.change > 0);
        const solReceivedAccounts = new Set(solReceivedChanges.map(change => change.account));

        // 找到签名者账户（用于匹配代币变化）
        const signerSolChanges = significantSolChanges.filter(change => change.isSigner);
        const signerAccounts = new Set(signerSolChanges.map(change => change.account));

        // 找到用户账户的代币变化（花费 SOL 的账户收到的代币，或收到 SOL 的账户失去的代币）
        const userTokenChanges = significantTokenChanges.filter(change => {
            // 买入：用户账户收到代币（花费 SOL 的账户收到代币，或签名者账户收到代币）
            if (change.change > 0) {
                if (solSpentAccounts.has(change.owner) || signerAccounts.has(change.owner)) {
                    return true;
                }
            }
            // 卖出：用户账户失去代币（收到 SOL 的账户失去代币，或签名者账户失去代币）
            if (change.change < 0) {
                if (solReceivedAccounts.has(change.owner) || signerAccounts.has(change.owner)) {
                    return true;
                }
            }
            return false;
        });

        // 如果找不到用户账户的代币变化，尝试使用签名者账户
        let effectiveTokenChanges = userTokenChanges;
        if (effectiveTokenChanges.length === 0) {
            const signerSolChanges = significantSolChanges.filter(change => change.isSigner);
            if (signerSolChanges.length > 0) {
                const signerAccounts = new Set(signerSolChanges.map(change => change.account));
                // 查找签名者账户的代币变化（包括新创建的账户，即只有 post 没有 pre 的情况）
                effectiveTokenChanges = significantTokenChanges.filter(change => 
                    signerAccounts.has(change.owner)
                );
            }
        }
        
        // 如果还是找不到，且是买入场景（有 SOL 花费），尝试查找所有正变化的代币（可能是新账户）
        if (effectiveTokenChanges.length === 0 && solSpentChanges.length > 0) {
            // 买入场景：用户花费 SOL，应该收到代币
            // 查找所有正变化的代币（可能是新创建的账户）
            const positiveTokenChanges = significantTokenChanges.filter(change => change.change > 0);
            if (positiveTokenChanges.length > 0) {
                // 优先使用绝对值最大的正变化（通常是用户收到的代币）
                effectiveTokenChanges = [positiveTokenChanges.reduce((max, change) => 
                    Math.abs(change.change) > Math.abs(max.change) ? change : max, positiveTokenChanges[0]
                )];
            }
        }

        // 如果还是找不到，使用所有代币变化，但只取绝对值最大的变化（通常是用户的实际变化）
        if (effectiveTokenChanges.length === 0 && significantTokenChanges.length > 0) {
            // 按 mint 分组，找到绝对值最大的变化
            const tokenChangesByMint = new Map();
            significantTokenChanges.forEach(change => {
                const key = change.mint;
                if (!tokenChangesByMint.has(key)) {
                    tokenChangesByMint.set(key, {
                        mint: change.mint,
                        decimals: change.decimals,
                        changes: []
                    });
                }
                tokenChangesByMint.get(key).changes.push(change);
            });

            // 对于每个代币，找到绝对值最大的变化
            tokenChangesByMint.forEach((tokenData, mint) => {
                const maxChange = tokenData.changes.reduce((max, change) => 
                    Math.abs(change.change) > Math.abs(max.change) ? change : max, tokenData.changes[0]
                );
                effectiveTokenChanges.push(maxChange);
            });
        }

        // 聚合同一代币的所有变化（按 mint 地址分组）
        const tokenChangeMap = new Map();
        effectiveTokenChanges.forEach(change => {
            const key = change.mint;
            if (!tokenChangeMap.has(key)) {
                tokenChangeMap.set(key, {
                    mint: change.mint,
                    decimals: change.decimals,
                    totalChange: 0
                });
            }
            tokenChangeMap.get(key).totalChange += change.change;
        });

        // 场景1: 用 SOL 买代币
        // 聚合所有负的 SOL 变化（所有账户花费的 SOL，包括手续费等）
        const totalSolSpent = solSpentChanges.reduce((sum, change) => sum + Math.abs(change.change), 0);
        
        // 找到所有买入的代币（变化为正的代币）
        const tokensBought = Array.from(tokenChangeMap.values()).filter(token => token.totalChange > 0);
        // 如果只有一个代币被买入，使用它；否则使用变化最大的代币
        const tokenBought = tokensBought.length === 1 
            ? tokensBought[0]
            : tokensBought.length > 0
            ? tokensBought.reduce((max, token) => token.totalChange > max.totalChange ? token : max, tokensBought[0])
            : null;

        if (totalSolSpent > 0 && tokenBought) {
            // 识别持有人地址：
            // 1. 优先使用收到代币的账户（effectiveTokenChanges 中的 owner）
            // 2. 如果找不到，使用签名者账户（即使它的 SOL 变化很小）
            // 3. 最后使用花费 SOL 的账户
            let holderAddress = null;
            
            // 优先使用收到代币的账户（通常是签名者）
            if (effectiveTokenChanges.length > 0) {
                // 找到收到买入代币的账户
                const tokenBoughtChanges = effectiveTokenChanges.filter(change => 
                    change.mint === tokenBought.mint && change.change > 0
                );
                if (tokenBoughtChanges.length > 0) {
                    // 优先使用签名者账户的代币变化
                    const signerTokenChange = tokenBoughtChanges.find(change => {
                        const solChange = solChanges.find(sc => sc.account === change.owner && sc.isSigner);
                        return solChange !== undefined;
                    });
                    holderAddress = signerTokenChange?.owner || tokenBoughtChanges[0]?.owner;
                }
            }
            
            // 如果还是找不到，使用签名者账户（从所有 SOL 变化中查找，包括变化很小的）
            if (!holderAddress) {
                const allSignerChange = solChanges.find(change => change.isSigner);
                if (allSignerChange) {
                    holderAddress = allSignerChange.account;
                }
            }
            
            // 如果还是找不到，使用花费 SOL 的账户
            if (!holderAddress) {
                holderAddress = solSpentChanges.find(change => change.isSigner)?.account 
                    || solSpentChanges[0]?.account 
                    || null;
            }
            
            return {
                type: 'buy',
                soldToken: {
                    mint: this.SOL_MINT,
                    symbol: 'SOL',
                    amount: totalSolSpent,
                    decimals: 9
                },
                boughtToken: {
                    mint: tokenBought.mint,
                    symbol: 'Token',
                    amount: tokenBought.totalChange,
                    decimals: tokenBought.decimals
                },
                price: totalSolSpent / tokenBought.totalChange,
                holderAddress: holderAddress
            };
        }

        // 场景2: 卖代币换 SOL
        // 聚合所有正的 SOL 变化（所有账户收到的 SOL）
        const totalSolReceived = solReceivedChanges.reduce((sum, change) => sum + change.change, 0);
        
        // 找到所有卖出的代币（变化为负的代币）
        const tokensSold = Array.from(tokenChangeMap.values()).filter(token => token.totalChange < 0);
        // 如果只有一个代币被卖出，使用它；否则使用变化最大的代币（绝对值）
        const tokenSold = tokensSold.length === 1
            ? tokensSold[0]
            : tokensSold.length > 0
            ? tokensSold.reduce((max, token) => Math.abs(token.totalChange) > Math.abs(max.totalChange) ? token : max, tokensSold[0])
            : null;

        if (totalSolReceived > 0 && tokenSold) {
            // 识别持有人地址：
            // 1. 优先使用卖出代币的账户（effectiveTokenChanges 中的 owner）
            // 2. 如果找不到，使用签名者账户（即使它的 SOL 变化很小）
            // 3. 最后使用收到 SOL 的账户
            let holderAddress = null;
            
            // 优先使用卖出代币的账户（通常是签名者）
            if (effectiveTokenChanges.length > 0) {
                // 找到卖出代币的账户
                const tokenSoldChanges = effectiveTokenChanges.filter(change => 
                    change.mint === tokenSold.mint && change.change < 0
                );
                if (tokenSoldChanges.length > 0) {
                    // 优先使用签名者账户的代币变化
                    const signerTokenChange = tokenSoldChanges.find(change => {
                        const solChange = solChanges.find(sc => sc.account === change.owner && sc.isSigner);
                        return solChange !== undefined;
                    });
                    holderAddress = signerTokenChange?.owner || tokenSoldChanges[0]?.owner;
                }
            }
            
            // 如果还是找不到，使用签名者账户（从所有 SOL 变化中查找，包括变化很小的）
            if (!holderAddress) {
                const allSignerChange = solChanges.find(change => change.isSigner);
                if (allSignerChange) {
                    holderAddress = allSignerChange.account;
                }
            }
            
            // 如果还是找不到，使用收到 SOL 的账户
            if (!holderAddress) {
                holderAddress = solReceivedChanges.find(change => change.isSigner)?.account
                    || (solReceivedChanges.length > 0 ? solReceivedChanges[0]?.account : null);
            }
            
            // 最后尝试从 accountKeys 中获取签名者
            if (!holderAddress && transaction.transaction?.message?.accountKeys) {
                const signerAccountKey = transaction.transaction.message.accountKeys.find(acc => {
                    if (typeof acc === 'object' && acc.signer) {
                        return true;
                    }
                    return false;
                });
                if (signerAccountKey) {
                    holderAddress = typeof signerAccountKey === 'string' 
                        ? signerAccountKey 
                        : (signerAccountKey.pubkey?.toString() || signerAccountKey.toString?.() || String(signerAccountKey));
                }
            }
            
            return {
                type: 'sell',
                soldToken: {
                    mint: tokenSold.mint,
                    symbol: 'Token',
                    amount: Math.abs(tokenSold.totalChange),
                    decimals: tokenSold.decimals
                },
                boughtToken: {
                    mint: this.SOL_MINT,
                    symbol: 'SOL',
                    amount: totalSolReceived,
                    decimals: 9
                },
                price: totalSolReceived / Math.abs(tokenSold.totalChange),
                holderAddress: holderAddress || null
            };
        }

        return null;
    }

    /**
     * 推断交易类型
     * @param {string} inputMint - 输入代币
     * @param {string} outputMint - 输出代币
     * @returns {string} 交易类型
     */
    inferTradeType(inputMint, outputMint) {
        if (inputMint === this.SOL_MINT) return 'buy';
        if (outputMint === this.SOL_MINT) return 'sell';
        return 'swap';
    }

    /**
     * 计算价格
     * @param {object} sold - 卖出代币
     * @param {object} bought - 买入代币
     * @returns {number} 价格
     */
    calculatePrice(sold, bought) {
        const soldAmount = Math.abs(sold.amount);
        const boughtAmount = bought.amount;

        if (soldAmount === 0) return 0;

        return boughtAmount / soldAmount;
    }

    /**
     * 解析代币余额变化
     * @param {object} meta - 交易元数据
     * @returns {array} 代币变化列表
     */
    parseTokenBalanceChanges(meta) {
        const changes = [];

        if (!meta?.postTokenBalances && !meta?.preTokenBalances) return changes;

        const preTokenBalances = meta.preTokenBalances || [];
        const postTokenBalances = meta.postTokenBalances || [];
        
        // 创建 pre token balances 的映射，使用 owner + mint 作为 key
        const preMap = new Map();
        preTokenBalances.forEach(pre => {
            if (pre.owner && pre.mint) {
                const key = `${pre.owner}-${pre.mint}`;
                preMap.set(key, pre);
            }
        });

        // 创建 post token balances 的映射
        const postMap = new Map();
        postTokenBalances.forEach(post => {
            if (post.owner && post.mint) {
                const key = `${post.owner}-${post.mint}`;
                postMap.set(key, post);
            }
        });

        // 使用 Set 来避免重复添加相同的代币变化
        const changesSet = new Map(); // 使用 owner-mint 作为 key 来去重
        
        // 遍历 post token balances，匹配对应的 pre balance
        postTokenBalances.forEach(post => {
            if (!post.owner || !post.mint) return;
            
            const key = `${post.owner}-${post.mint}`;
            const pre = preMap.get(key);
            
            const postAmount = post.uiTokenAmount?.uiAmount || 0;
            const preAmount = pre?.uiTokenAmount?.uiAmount || 0;
            
            // 如果余额有变化，记录变化
            if (Math.abs(postAmount - preAmount) > 0.000001) {
                changesSet.set(key, {
                    mint: post.mint,
                    owner: post.owner,
                    preAmount: preAmount,
                    postAmount: postAmount,
                    change: postAmount - preAmount,
                    decimals: post.uiTokenAmount?.decimals || 0
                });
            }
        });

        // 检查 pre token balances 中消失的账户（从有到无的情况，通常是卖出代币）
        preTokenBalances.forEach(pre => {
            if (!pre.owner || !pre.mint) return;
            
            const key = `${pre.owner}-${pre.mint}`;
            const post = postMap.get(key);
            
            const preAmount = pre.uiTokenAmount?.uiAmount || 0;
            const postAmount = post?.uiTokenAmount?.uiAmount || 0;
            
            // 如果 pre 有余额但 post 没有（账户消失），记录为负变化（卖出）
            if (preAmount > 0.000001 && (!post || postAmount < 0.000001)) {
                // 如果已经存在，更新它；否则添加新的
                if (!changesSet.has(key)) {
                    changesSet.set(key, {
                        mint: pre.mint,
                        owner: pre.owner,
                        preAmount: preAmount,
                        postAmount: 0,
                        change: -preAmount, // 负变化表示卖出
                        decimals: pre.uiTokenAmount?.decimals || 0
                    });
                }
            }
        });

        // 检查 post token balances 中新创建的账户（从无到有的情况，通常是买入代币）
        // 这些账户在 preTokenBalances 中不存在，但在 postTokenBalances 中存在
        postTokenBalances.forEach(post => {
            if (!post.owner || !post.mint) return;
            
            const key = `${post.owner}-${post.mint}`;
            const pre = preMap.get(key);
            
            const postAmount = post.uiTokenAmount?.uiAmount || 0;
            const preAmount = pre?.uiTokenAmount?.uiAmount || 0;
            
            // 如果 post 有余额但 pre 不存在（新账户），记录为正变化（买入）
            // 注意：第一个循环可能已经处理了这种情况（preAmount=0），但为了确保，这里也检查
            if (postAmount > 0.000001 && !pre) {
                // 如果已经存在，更新它；否则添加新的
                if (!changesSet.has(key)) {
                    changesSet.set(key, {
                        mint: post.mint,
                        owner: post.owner,
                        preAmount: 0,
                        postAmount: postAmount,
                        change: postAmount, // 正变化表示买入
                        decimals: post.uiTokenAmount?.decimals || 0
                    });
                }
            }
        });

        // 将 Map 转换为数组
        changes.push(...Array.from(changesSet.values()));

        return changes;
    }

    /**
     * 解析 SOL 余额变化
     * @param {object} transaction - 交易数据
     * @returns {array} SOL 变化列表
     */
    parseSOLBalanceChanges(transaction) {
        const { meta, transaction: { message } } = transaction;
        const changes = [];

        if (!meta?.postBalances) return changes;

        // 获取签名者数量（根据 Solana 规范，前 numRequiredSignatures 个账户是签名者）
        const numRequiredSignatures = message.header?.numRequiredSignatures || 0;
        const numReadonlySignedAccounts = message.header?.numReadonlySignedAccounts || 0;
        // 签名者账户索引范围：0 到 (numRequiredSignatures - 1)
        const signerEndIndex = numRequiredSignatures;

        // 构建完整的账户地址列表（包括 loadedAddresses）
        const allAccountKeys = [...(message.accountKeys || [])];
        const loadedAddresses = meta.loadedAddresses || {};
        const writableLoaded = loadedAddresses.writable || [];
        const readonlyLoaded = loadedAddresses.readonly || [];
        const allLoadedAddresses = [...writableLoaded, ...readonlyLoaded];
        
        // 将 loadedAddresses 添加到账户列表
        allLoadedAddresses.forEach(addr => {
            allAccountKeys.push(addr);
        });

        meta.postBalances.forEach((postBalance, index) => {
            const preBalance = meta.preBalances[index];

            if (preBalance !== postBalance) {
                const accountKey = allAccountKeys[index];
                
                // 安全地获取账户地址（处理不同的数据格式）
                let accountAddress = null;
                let isSigner = false;
                let isWritable = false;
                
                if (accountKey) {
                    if (typeof accountKey === 'string') {
                        accountAddress = accountKey;
                        // 如果是字符串格式，根据索引判断是否是签名者
                        isSigner = index < signerEndIndex;
                    } else if (accountKey && typeof accountKey === 'object') {
                        accountAddress = accountKey.pubkey?.toString() || accountKey.toString?.() || String(accountKey);
                        // 优先使用对象中的 signer 属性，如果没有则根据索引判断
                        isSigner = accountKey.signer !== undefined ? accountKey.signer : (index < signerEndIndex);
                        isWritable = accountKey.writable || false;
                    } else {
                        accountAddress = String(accountKey);
                        isSigner = index < signerEndIndex;
                    }
                } else {
                    // 如果 accountKey 不存在，尝试从 loadedAddresses 获取
                    const loadedIndex = index - (message.accountKeys?.length || 0);
                    if (loadedIndex >= 0 && loadedIndex < allLoadedAddresses.length) {
                        accountAddress = allLoadedAddresses[loadedIndex];
                        isSigner = false; // loadedAddresses 中的账户不是签名者
                    } else {
                        // 如果还是找不到，使用索引作为占位符（但这种情况应该很少）
                        accountAddress = `account_${index}`;
                        isSigner = index < signerEndIndex;
                    }
                }
                
                changes.push({
                    account: accountAddress,
                    preBalance: preBalance / 1e9,
                    postBalance: postBalance / 1e9,
                    change: (postBalance - preBalance) / 1e9,
                    isSigner: isSigner,
                    isWritable: isWritable
                });
            }
        });

        return changes;
    }

    /**
     * 批量解析交易
     * @param {array} signatures - 交易签名数组
     * @param {object} options - 选项
     * @returns {Promise<array>} 交易信息数组
     */
    async parseMultipleTrades(signatures, options = {}) {
        const { delay = 100, ...parseOptions } = options;
        const results = [];

        for (const signature of signatures) {
            try {
                const tradeInfo = await this.parseTrade(signature, parseOptions);
                if (tradeInfo) {
                    results.push(tradeInfo);
                }

                // 避免请求过快
                if (delay > 0) {
                    await new Promise(resolve => setTimeout(resolve, delay));
                }
            } catch (error) {
                console.error(`Error parsing trade ${signature}:`, error);
            }
        }

        return results;
    }

    /**
     * 监控特定 DEX 的新交易
     * @param {string} dex - DEX 名称
     * @param {function} callback - 回调函数
     * @returns {number} 订阅ID
     */
    monitorDexTrades(dex, callback) {
        const programIds = this.DEX_PROGRAMS[dex.toUpperCase()];

        if (!programIds || programIds.length === 0) {
            throw new Error(`Unsupported DEX: ${dex}`);
        }

        const subscriptionId = this.connection.onLogs(
            new PublicKey(programIds[0]),
            async (logs, context) => {
                if (!logs.err) {
                    try {
                        const tradeInfo = await this.parseTrade(context.signature);
                        if (tradeInfo) {
                            callback(tradeInfo);
                        }
                    } catch (error) {
                        console.error('Error processing monitored trade:', error);
                    }
                }
            },
            'confirmed'
        );

        return subscriptionId;
    }

    /**
     * 取消监控
     * @param {number} subscriptionId - 订阅ID
     */
    unsubscribe(subscriptionId) {
        this.connection.removeOnLogsListener(subscriptionId);
    }
}

// 导出类
export default SolanaDexTradeParser;

// 使用示例
/*
// 初始化解析器
const parser = new SolanaDexTradeParser();

// 解析单个交易
async function example() {
  const signature = '你的交易签名';
  const tradeInfo = await parser.parseTrade(signature);

  if (tradeInfo) {
    console.log('交易信息:', {
      平台: tradeInfo.dex,
      类型: tradeInfo.type,
      卖出: `${tradeInfo.soldToken.amount} ${tradeInfo.soldToken.symbol}`,
      买入: `${tradeInfo.boughtToken.amount} ${tradeInfo.boughtToken.symbol}`,
      价格: tradeInfo.price,
      时间: tradeInfo.timestamp
    });
  }
}

// 批量解析
async function batchExample() {
  const signatures = ['签名1', '签名2', '签名3'];
  const trades = await parser.parseMultipleTrades(signatures, { delay: 200 });
  console.log(`成功解析 ${trades.length} 笔交易`);
}

// 实时监控
function monitorExample() {
  const subscriptionId = parser.monitorDexTrades('raydium', (tradeInfo) => {
    console.log('新的 Raydium 交易:', tradeInfo);
  });

  // 取消监控
  // parser.unsubscribe(subscriptionId);
}
*/