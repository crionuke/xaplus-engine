package org.xaplus.engine;

import com.crionuke.bolts.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusThreadPool {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusThreadPool.class);
    static private final int THREAD_POOL_SIZE = 32;

    private final ExecutorService threadPool;

    XAPlusThreadPool() {
        threadPool = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        logger.info("Thread pool with size={} created", THREAD_POOL_SIZE);
    }

    void execute(Worker worker) {
        threadPool.execute(worker);
    }
}
