package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveryFinishedEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveryPreparedEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveryTimedOutEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.timer.XAPlusTimerCancelledEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;

import java.util.List;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusTimerService extends Bolt implements
        XAPlusRecoveryPreparedEvent.Handler,
        XAPlusRecoveryFinishedEvent.Handler,
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusRollbackDoneEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusTickEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTimerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTimerTracker tracker;

    XAPlusTimerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                       XAPlusTimerTracker tracker) {
        super(properties.getServerId() + "-timer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.tracker = tracker;
    }

    @Override
    public void handleRecoveryPrepared(XAPlusRecoveryPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        tracker.recoveryStarted();
    }

    @Override
    public void handleRecoveryFinished(XAPlusRecoveryFinishedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        tracker.resetRecoveryTracker();
    }

    @Override
    public void handleUserCommitRequest(XAPlusUserCommitRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Track timeout, {}", transaction);
            }
        }
    }

    @Override
    public void handleUserRollbackRequest(XAPlusUserRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Track timeout, {}", transaction);
            }
        }
    }

    @Override
    public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.remove(transaction.getXid())) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout tracking cancelled as 2pc procotol done, {}", transaction);
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
        if (tracker.remove(transaction.getXid())) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout tracking cancelled as 2pc procotol failed, {}", transaction);
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
        if (tracker.remove(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout tracking cancelled as rollback protocol done, {}", transaction);
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
        if (tracker.remove(xid)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Timeout tracking cancelled as rollback protocol failed, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTimerCancelledEvent(transaction));
        }
    }

    @Override
    public void handleTick(XAPlusTickEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        List<XAPlusTransaction> expiredTransaction = tracker.removeExpiredTransactions();
        if (logger.isDebugEnabled()) {
            if (expiredTransaction.size() > 0) {
                logger.debug("Found {} expired transactions", expiredTransaction.size());
            }
        }
        for (XAPlusTransaction transaction : expiredTransaction) {
            dispatcher.dispatch(new XAPlusTransactionTimedOutEvent(transaction));
        }
        if (tracker.isRecoveryTimedOut()) {
            dispatcher.dispatch(new XAPlusRecoveryTimedOutEvent());
            tracker.resetRecoveryTracker();
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRecoveryPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveryFinishedEvent.class);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTickEvent.class);
    }
}