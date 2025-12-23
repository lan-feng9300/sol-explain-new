/**
 * 应用配置
 */
export const config = {
  // 服务器配置
  server: {
    port: process.env.PORT || 3000,
  },

  // Solana RPC 端点
  solana: {
    // 优先级：dRPC > QuickNode > Ankr > Helius > 默认公共 RPC
    rpcEndpoint: process.env.DRPC_API_KEY
      ? `https://lb.drpc.live/solana/${process.env.DRPC_API_KEY}`
      : (process.env.QUICKNODE_API_KEY
        ? `https://${process.env.QUICKNODE_ENDPOINT || 'your-endpoint'}.solana-mainnet.quiknode.pro/${process.env.QUICKNODE_API_KEY}/`
        : (process.env.ANKR_API_KEY
          ? `https://rpc.ankr.com/solana/${process.env.ANKR_API_KEY}`
      : (process.env.HELIUS_API_KEY 
        ? `https://mainnet.helius-rpc.com/?api-key=${process.env.HELIUS_API_KEY}`
            : (process.env.SOLANA_RPC_ENDPOINT || 'https://api.mainnet-beta.solana.com')))),
  },

  // dRPC API Key（优先使用，用于地址交易历史查询和 WebSocket 订阅）
  drpc: {
    apiKey: process.env.DRPC_API_KEY || '',
  },

  // Helius API Key（可选，如果有的话）
  // 从环境变量读取，不要硬编码 API Key（安全考虑）
  helius: {
    apiKey: process.env.HELIUS_API_KEY || '',
  },

  // Birdeye API Key（用于钱包盈亏分析）
  birdeye: {
    apiKey: process.env.BIRDEYE_API_KEY || '',
  },

  mysql: {
    HOST: process.env.MYSQL_HOST || "localhost",
    USER: process.env.MYSQL_USER || "root",
    PASSWORD: process.env.MYSQL_PASSWORD || "",
    DB: process.env.MYSQL_DB || "sol"
  },

  // Redis 配置（用于任务队列）
  redis: {
    HOST: process.env.REDIS_HOST || "localhost",
    PORT: process.env.REDIS_PORT || 6379,
    PASSWORD: process.env.REDIS_PASSWORD || "",
  },

  walletAddress: {
    PRIVATE_KEY: process.env.PRIVATE_KEY || '',
  },
};

