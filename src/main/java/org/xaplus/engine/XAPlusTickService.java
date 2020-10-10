package org.xaplus.engine;

import com.crionuke.bolts.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.XAPlusTickEvent;

import javax.annotation.PostConstruct;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusTickService extends Worker {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTickService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;

    XAPlusTickService(XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        this.threadPool = threadPool;
    }

    @Override
    public void run() {
        String oldThreadName = Thread.currentThread().getName();
        Thread.currentThread().setName("tick-" + uid);
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

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
    }
}
