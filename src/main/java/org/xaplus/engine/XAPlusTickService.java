package org.xaplus.engine;

import com.crionuke.bolts.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusTickService extends Worker {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTickService.class);

    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;

    XAPlusTickService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        this.properties = properties;
        this.dispatcher = dispatcher;
        this.threadPool = threadPool;
    }

    @Override
    public void run() {
        String oldThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName(properties.getServerId() + "-tick-" + uid);
        logger.debug("{} started", this);
        looping = true;
        try {
            int index = 0;
            while (looping) {
                index++;
                dispatcher.dispatch(new XAPlusTickEvent(index));
                Thread.sleep(1000);
            }
        } catch (InterruptedException ie) {
            logger.debug("{} interrupted", this);
            looping = false;
        }
        logger.debug("{} finished", this);
        Thread.currentThread().setName(oldThreadName);
    }

    void postConstruct() {
        threadPool.execute(this);
    }
}
