import express from 'express';
import {
  getTokenTransactionsHandler,
  getPoolTransactionsHandler,
  getAddressTransactionsHandler,
  getTokenHoldersHandler,
  getTokenHoldersV2Handler,
  getTokenHoldersSummaryHandler,
  getAddressTransactionHistoryHandler,
  parseTransactionBuySellInfoHandler,
  getWalletPnlDetailsHandler,
  getTransactionsForAddressHandler,
  syncTradesToDatabaseHandler,
  getWalletPnlDetailsByHeliusTestHandler
} from '../controllers/tokenController.js';


import {
  getWalletPnlDetailsByHeliusHandler,
  getWalletPnlDetailsByHeliusTypeHandler,
  syncAnalyzeHolderPnlByHeliusSwapTypeHandler,
  syncAnalyzeHolderPnlHandler
} from '../controllers/holderAddrController.js';

const router = express.Router();

/**
 * 获取代币交易记录的 API 端点
 * 支持两种方式：
 * 1. 使用 Helius API（推荐，需要 API key）
 * 2. 使用基础 RPC（功能有限）
 */
router.get('/token/:tokenAddress', getTokenTransactionsHandler);

/**
 * 通过交易对地址获取交易（适用于已知的 DEX 交易对）
 */
router.get('/pool/:poolAddress', getPoolTransactionsHandler);

/**
 * 获取特定地址的交易记录
 */
router.get('/address/:address', getAddressTransactionsHandler);

/**
 * 获取代币持有人列表
 * 支持分页查询: ?offset=0&limit=100
 * 支持地址聚类: ?enableClustering=true (较慢，但能识别关联地址)
 * 使用 Helius API 获取持有人数据（需要配置 HELIUS_API_KEY）
 */
router.get('/token/:tokenAddress/holders', getTokenHoldersHandler);

/**
 * 获取代币持有人列表（使用 Helius getProgramAccountsV2，支持分页获取所有账户）
 * 支持过滤和聚类: ?filterPools=true&checkOnChain=true&enableClustering=true
 * 使用 Helius getProgramAccountsV2 方法，支持分页获取最多 10,000 个账户
 * 文档: https://www.helius.dev/docs/rpc/guides/getprogramaccounts
 */
router.get('/token/:tokenAddress/holders/v2', getTokenHoldersV2Handler);

/**
 * 获取代币持仓占比汇总（按批次时间分组）
 * 用于绘制折线图，展示不同时间点的持仓占比变化
 */
router.get('/token/:tokenAddress/holders/summary', getTokenHoldersSummaryHandler);

/**
 * 获取地址的详细交易历史（使用 Helius RPC）
 * 支持查询钱包、程序ID、代币铸币地址等的交易历史
 * 参考文档: https://learnblockchain.cn/article/11171
 * 查询参数: ?limit=100 (默认100，最大1000)
 */
router.get('/address/:address/transactions/history', getAddressTransactionHistoryHandler);

/**
 * 使用 Helius 增强 API getTransactionsForAddress 获取地址交易历史
 * 支持高级过滤、排序和分页功能
 * 文档: https://www.helius.dev/docs/api-reference/rpc/http/gettransactionsforaddress
 * 查询参数:
 *   - limit: 返回数量（默认 50，最大 1000）
 *   - transactionDetails: 'signatures'（仅签名）或 'full'（完整详情）
 *   - sortOrder: 'asc'（升序）或 'desc'（降序，默认）
 *   - status: 'succeeded'（成功）或 'failed'（失败）
 *   - slotGte: Slot 起始值
 *   - slotLt: Slot 结束值
 *   - blockTimeGte: 时间戳起始值（默认按北京时间处理，会自动转换为 UTC）
 *   - blockTimeLt: 时间戳结束值（默认按北京时间处理，会自动转换为 UTC）
 *   - skipTimezoneConversion: 如果传入的已经是 UTC 时间戳，设置为 true 或 1 跳过时区转换（默认会自动转换）
 *   - paginationToken: 分页令牌（用于获取下一页）
 * 示例: 
 *   GET /api/address/:address/transactions?limit=100&sortOrder=desc&status=succeeded
 *   GET /api/address/:address/transactions?blockTimeGte=1641038400&blockTimeLt=1641038460
 */
router.get('/address/:address/transactions', getTransactionsForAddressHandler);


/**
 * 测试路由：解析交易的买入/卖出信息
 * 整合了多种解析方法：
 * 1. Jupiter API（优先级最高）
 * 2. Raydium 交易解析
 * 3. 余额变化分析（兜底方案）
 * 入参：交易签名
 * 示例: GET /api/transaction/:signature/buy-sell-info
 */
router.get('/transaction/:signature/buy-sell-info', parseTransactionBuySellInfoHandler);

/**
 * 获取钱包代币盈亏详情（使用 Birdeye API）
 * 文档: https://public-api.birdeye.so/wallet/v2/pnl/details
 * 示例: GET /api/wallet/:walletAddress/pnl/details
 * 查询参数: ?token=TOKEN_ADDRESS&offset=0&limit=100
 */
router.get('/wallet/:walletAddress/pnl/details', getWalletPnlDetailsHandler);

/**
 * 获取钱包代币盈亏详情（使用 helius API）
 * 文档: https://www.helius.dev/docs/api-reference/enhanced-transactions/gettransactionsbyaddress
 *
 *
 */
router.get('/wallet/:walletAddress/pnl/byHelius/details', getWalletPnlDetailsByHeliusHandler);

/**
 *  通过 type = "swap" 过滤
 *  这个接口：https://www.helius.dev/docs/api-reference/enhanced-transactions/gettransactionsbyaddress
 */
router.get('/wallet/:walletAddress/pnl/byHeliusType/details', getWalletPnlDetailsByHeliusTypeHandler);

/**
 * 同步分析持有者盈亏
 * 对 tbl_sol_trade_info 表中的代币和地址进行归总分析
 * 对每个地址调用 Birdeye API 获取盈亏详情，根据过滤条件决定是否保存
 * minAmount: 买入代币的数量，默认 1.5m
 * minPnlUsd: 100个代币内盈利的总金额，默认 0.5wu
 * 示例: GET /api/sync-analyze-holder-pnl
 *      GET /api/sync-analyze-holder-pnl?minAmount=2000000?minPnlUsd=5000
 */
router.get('/sync-analyze-holder-pnl', syncAnalyzeHolderPnlHandler);

/**
 * 同步分析持有者盈亏
 * 功能同上，只是这里用了helius的swap过滤接口查询地址交易详情
 */
router.get('/byHeliusSwapType/sync-analyze-holder-pnl', syncAnalyzeHolderPnlByHeliusSwapTypeHandler);

/**
 * 自动同步交易到数据库
 * 根据时间范围自动循环获取所有交易，解析买入交易并保存到数据库
 * 只保存买入金额 >= 0.5 SOL 的交易，卖出不记录
 * 查询参数:
 *   - blockTimeGte: 时间戳起始值（Unix 时间戳，秒，必填，默认按北京时间处理，会自动转换为 UTC）
 *   - blockTimeLt: 时间戳结束值（Unix 时间戳，秒，必填，默认按北京时间处理，会自动转换为 UTC）
 *   - skipTimezoneConversion: 如果传入的已经是 UTC 时间戳，设置为 true 或 1 跳过时区转换（默认会自动转换）
 *   - maxPages: 最大页数限制（默认 300）
 *   - maxDuration: 最大持续时间（毫秒，默认 900000，即 15 分钟）
 * 安全机制:
 *   - 自动检测客户端断开连接并停止
 *   - 达到最大页数或最大持续时间时自动停止
 *   - 如果因限制停止，返回 paginationToken 以便继续同步
 * 示例: 
 *   GET /api/address/:address/sync-trades?blockTimeGte=1641038400&blockTimeLt=1641038460
 *   GET /api/address/:address/sync-trades?blockTimeGte=1641038400&blockTimeLt=1641038460&skipTimezoneConversion=true
 */
router.get('/address/:address/sync-trades', syncTradesToDatabaseHandler);

router.get('/address/:walletAddress/testHelius', getWalletPnlDetailsByHeliusTestHandler);

export default router;

