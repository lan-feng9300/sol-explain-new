import { checkMemoryUsage } from '../utils/memoryMonitor.js';

/**
 * 内存保护中间件
 * 防止内存溢出导致服务崩溃
 */
export function memoryProtectionMiddleware(req, res, next) {
    const memoryStatus = checkMemoryUsage();

    // 如果内存使用超过85%，返回503服务暂时不可用
    if (memoryStatus.usagePercent > 85) {
        console.error(`🚨 内存保护触发: 使用率 ${memoryStatus.usagePercent.toFixed(1)}%`);

        return res.status(503).json({
            success: false,
            error: '服务器资源紧张，请稍后再试',
            memoryUsage: `${memoryStatus.heapUsedMB.toFixed(2)} MB`,
            usagePercent: `${memoryStatus.usagePercent.toFixed(1)}%`
        });
    }

    next();
}

/**
 * 为长时间运行任务优化的中间件
 */
export function longRunningTaskProtection(req, res, next) {
    // 设置响应超时
    res.setTimeout(300000, () => { // 5分钟超时
        console.warn(`⏰ 请求超时: ${req.path}`);
        if (!res.headersSent) {
            res.status(504).json({
                success: false,
                error: '请求处理超时'
            });
        }
    });

    next();
}

/**
 * 安全执行函数，带内存保护
 * 添加到这个文件中，因为它属于内存保护相关功能
 */
export async function executeWithMemoryProtection(fn, options = {}) {
    const {
        maxHeapMB = 800,
        checkInterval = 1000,
        label = 'Memory Protected Execution'
    } = options;

    let memoryCheckInterval;
    let isCancelled = false;

    // 启动内存监控
    memoryCheckInterval = setInterval(() => {
        const mem = process.memoryUsage();
        const heapMB = mem.heapUsed / 1024 / 1024;

        if (heapMB > maxHeapMB && !isCancelled) {
            console.warn(`🚨 ${label}: 内存超过 ${maxHeapMB}MB (当前: ${heapMB.toFixed(2)}MB)`);
            isCancelled = true;
        }
    }, checkInterval);

    try {
        const result = await fn();
        clearInterval(memoryCheckInterval);
        return { success: true, data: result };
    } catch (error) {
        clearInterval(memoryCheckInterval);
        return {
            success: false,
            error: error.message,
            cancelled: isCancelled
        };
    }
}