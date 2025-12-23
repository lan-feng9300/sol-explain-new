import v8 from 'v8';

/**
 * 记录详细的内存使用情况
 */
export function logMemoryUsage(label = 'Memory Usage') {
    const memoryUsage = process.memoryUsage();
    const heapStats = v8.getHeapStatistics();

    console.log(`🧠 ${label}:
    RSS: ${(memoryUsage.rss / 1024 / 1024).toFixed(2)} MB
    Heap Total: ${(memoryUsage.heapTotal / 1024 / 1024).toFixed(2)} MB
    Heap Used: ${(memoryUsage.heapUsed / 1024 / 1024).toFixed(2)} MB
    External: ${(memoryUsage.external / 1024 / 1024).toFixed(2)} MB
    Array Buffers: ${(memoryUsage.arrayBuffers / 1024 / 1024).toFixed(2)} MB
    Heap Limit: ${(heapStats.heap_size_limit / 1024 / 1024).toFixed(2)} MB
    Used Heap: ${(heapStats.used_heap_size / 1024 / 1024).toFixed(2)} MB
    Heap Usage: ${((heapStats.used_heap_size / heapStats.heap_size_limit) * 100).toFixed(1)}%`);

    return {
        heapUsedMB: memoryUsage.heapUsed / 1024 / 1024,
        heapUsagePercent: (heapStats.used_heap_size / heapStats.heap_size_limit) * 100
    };
}

/**
 * 检查内存使用情况，如果过高返回警告
 */
export function checkMemoryUsage() {
    const mem = process.memoryUsage();
    const heapStats = v8.getHeapStatistics();
    const usagePercent = (heapStats.used_heap_size / heapStats.heap_size_limit) * 100;

    const status = {
        heapUsedMB: mem.heapUsed / 1024 / 1024,
        heapTotalMB: mem.heapTotal / 1024 / 1024,
        usagePercent,
        isCritical: usagePercent > 85
    };

    if (status.isCritical) {
        console.warn(`⚠️ 内存使用过高: ${usagePercent.toFixed(1)}% (${status.heapUsedMB.toFixed(2)} MB)`);
    }

    return status;
}

/**
 * 安全执行函数，带内存保护
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