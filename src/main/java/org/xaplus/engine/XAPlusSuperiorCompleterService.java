package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusCompletedTransactionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogComplettedTransactionFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;

class XAPlusSuperiorCompleterService extends Bolt implements
        XAPlusCompletedTransactionLoggedEvent.Handler,
        XAPlusLogComplettedTransactionFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSuperiorCompleterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;

    XAPlusSuperiorCompleterService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                   XAPlusDispatcher dispatcher) {
        super(properties.getServerId() + "-superior-completer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
    }

    @Override
    public void handleCompletedTransactionLogged(XAPlusCompletedTransactionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior()) {
            if (event.getStatus()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("2pc done, {}", transaction);
                }
                dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Rollback done, {}", transaction);
                }
                dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
            }
        }
    }

    @Override
    public void handleLogCompletedTransactionFailed(XAPlusLogComplettedTransactionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior()) {
            if (event.getStatus()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("2pc failed, {}", transaction);
                }
                dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction));
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Rollback failed, {}", transaction);
                }
                dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction));
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusCompletedTransactionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusLogComplettedTransactionFailedEvent.class);
    }
}
