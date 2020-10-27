package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;
import org.xaplus.engine.events.timer.XAPlusTimerCancelledEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;

import java.util.List;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusTimerService extends Bolt implements
        XAPlus2pcRequestEvent.Handler,
        XAPlusRollbackRequestEvent.Handler,
        XAPlusTickEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlusRollbackDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusRollbackFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTimerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTimerState state;

    XAPlusTimerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                       XAPlusTimerState state) {
        super(properties.getServerId() + "-timer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.state = state;
    }

    @Override
    public void handle2pcRequest(XAPlus2pcRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (state.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Track timeout, {}", transaction);
            }
        }
    }

    @Override
    public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (state.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Track timeout, {}", transaction);
            }
        }
    }

    @Override
    public void handleTick(XAPlusTickEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        List<XAPlusTransaction> expiredTransaction = state.removeExpiredTransactions();
        for (XAPlusTransaction transaction : expiredTransaction) {
            dispatcher.dispatch(new XAPlusTransactionTimedOutEvent(transaction));
        }
    }

    @Override
    public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (state.remove(transaction.getXid())) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout tracking cancelled as 2pc procotol done, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTimerCancelledEvent(transaction));
        }
    }

    @Override
    public void handleRollbackDone(XAPlusRollbackDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        if (state.remove(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout tracking cancelled as rollback protocol done, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTimerCancelledEvent(transaction));
        }
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        if (state.remove(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout tracking cancelled as 2pc protocol failed, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTimerCancelledEvent(transaction));
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        if (state.remove(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout tracking cancelled as rollback protocol failed, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTimerCancelledEvent(transaction));
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusTickEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
    }
}