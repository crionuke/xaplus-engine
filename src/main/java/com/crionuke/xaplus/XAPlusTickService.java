package com.crionuke.xaplus;

import com.crionuke.bolts.Worker;
import com.crionuke.xaplus.events.XAPlusTickEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
final class XAPlusTickService extends Worker {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTickService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;

    XAPlusTickService(XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        this.dispatcher = dispatcher;
        this.threadPool = threadPool;
    }

    @Override
    public void run() {
        setThreadNamePrefix("tick");
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
        resetThreadName();
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
    }
}
