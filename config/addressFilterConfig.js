/**
 * 地址过滤配置文件
 * 通过配置列表来管理需要过滤的地址和程序 ID
 */

export const addressFilterConfig = {
  // DEX 程序 ID 列表（这些程序创建的账户会被过滤）
  dexProgramIds: [
    // Raydium
    '675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8', // Raydium V4
    '27haf8L6oxUeXrHrgEgsexjSY5hbVUWEmvv9Nyxg8vQv', // Raydium V3
    'CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK', // Raydium AMM V4 / CLMM
    '5quBtoiQqxF9Jv6KYKctB59NT3gtJD2Y65kdnB1Uev3h', // Raydium Stable
    '5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1', // Raydium Vault Authority
    
    // Orca
    'whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc', // Orca Whirlpool
    '9W959DqEETiGZocYWCQPaJ6sBmUzgfxXfqGeTEdp3aQP', // Orca Swap V2
    'DjVE6JNiYqPL2QXyCUUh8rNjHrbz9hXHNYt99MQ59qw1', // Orca Swap V1
    
    // Jupiter
    'JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4', // Jupiter V6
    'JUP4Fb2cqiRUcaTd8t5VhYu6oV5E2hbN8FdY3YbwPEsu', // Jupiter V4
    
    // Serum
    '9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin', // Serum DEX V3
    'EUqojwWA2rd19FZrzeB2J6YvWEbN5JqF7VKqJqJqJqJq', // Serum DEX V2
    
    // Pump.fun / Bonk 流动性池（通过 owner 检查过滤 Pump.fun 的地址）
    'GpMZbSM2GgvTKHJirzeGfMFoaZ8UR2X7F4v8vHTvxFbL', // Pump.fun/Bonk 流动性池程序 ID
    
    // Meteora 流动性池（通过 owner 检查过滤 Meteora 的地址）
    'HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC', // Meteora Pool Authority
  ],

  // 已知的流动性池地址列表（直接过滤这些地址，如 Bonk 的流动性池地址）
  knownLiquidityPoolAddresses: [
    'GpMZbSM2GgvTKHJirzeGfMFoaZ8UR2X7F4v8vHTvxFbL', // Bonk/Pump.fun 流动性池地址
    'HLnpSz9h2S4hiLQ43rnSD9XkcUThA7B8hQMKmDaiTLcC', // Meteora Pool Authority
  ],

  // 已知的 CEX 地址列表（直接过滤这些地址）
  cexAddresses: [
    // Binance 热钱包（示例，需要更新为真实地址）
    // '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM',
    // 'GJTxcnA5Sydy4YxE1BL2KT6hKsaH2i2J2KsaH2i2J2Ksa',
    
    // Coinbase（示例）
    // 可以在这里添加更多已知的 CEX 地址
  ],

  // 系统程序（通常不需要过滤，但可以在这里配置）
  systemPrograms: [
    '11111111111111111111111111111111', // System Program
    'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', // Token Program
    'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb', // Token-2022 Program
    'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL', // Associated Token Program
  ],
};

