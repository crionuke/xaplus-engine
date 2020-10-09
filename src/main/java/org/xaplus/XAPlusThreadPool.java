package org.xaplus;

import com.crionuke.bolts.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Component
class XAPlusThreadPool {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusThreadPool.class);
    static private final int THREAD_POOL_SIZE = 32;

    private final ThreadPoolTaskExecutor threadPoolTaskExecutor;

    XAPlusThreadPool() {
        threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setThreadNamePrefix("xaplus-");
        threadPoolTaskExecutor.setCorePoolSize(THREAD_POOL_SIZE);
        threadPoolTaskExecutor.initialize();
        logger.info("Thread pool with size={} created", THREAD_POOL_SIZE);
    }

    void execute(Worker service) {
        threadPoolTaskExecutor.execute(service);
    }
}
