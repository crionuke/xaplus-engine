package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusLogRollbackTransactionDecisionFailedEvent;
import org.xaplus.engine.events.journal.XAPlusRollbackTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.xa.XAPlusBranchRolledBackEvent;
import org.xaplus.engine.events.xa.XAPlusRollbackBranchFailedEvent;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusSuperiorRollbackService extends Bolt implements
        XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
        XAPlusLogRollbackTransactionDecisionFailedEvent.Handler,
        XAPlusBranchRolledBackEvent.Handler,
        XAPlusRollbackBranchFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSuperiorRollbackService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTracker tracker;

    XAPlusSuperiorRollbackService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                  XAPlusTracker tracker) {
        super(properties.getServerId() + "-superior-rollback", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.tracker = tracker;
    }

    @Override
    public void handleRollbackTransactionDecisionLogged(XAPlusRollbackTransactionDecisionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior() && tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback transaction, {}", transaction);
            }
            transaction.reset();
            transaction.rollback(dispatcher);
        }
    }

    @Override
    public void handleLogRollbackTransactionDecisionFailed(XAPlusLogRollbackTransactionDecisionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback failed, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction));
        }
    }

    @Override
    public void handleBranchRolledBack(XAPlusBranchRolledBackEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            XAPlusXid branchXid = event.getBranchXid();
            if (logger.isDebugEnabled()) {
                logger.debug("Branch rolled back, branchXid={}, {}", branchXid, transaction);
            }
            transaction.branchRolledBack(branchXid);
            check(transaction);
        }
    }

    @Override
    public void handleRollbackBranchFailed(XAPlusRollbackBranchFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            XAPlusXid branchXid = event.getBranchXid();
            if (logger.isDebugEnabled()) {
                logger.debug("Branch rollback failed, branchXid={}, {}", branchXid, transaction);
            }
            transaction.branchRolledBack(branchXid);
            transaction.branchFailed(branchXid);
            check(transaction);
        }
    }

    @Override
    public void handleTransactionTimedOut(XAPlusTransactionTimedOutEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction removed as timed out, {}", transaction);
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionFailedEvent.class);
        dispatcher.subscribe(this, XAPlusBranchRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
    }

    void check(XAPlusTransaction transaction) throws InterruptedException {
        if (transaction.isRollbackDone()) {
            tracker.remove(transaction.getXid());
            if (transaction.hasFailures()) {
                dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction));
            } else {
                dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
            }
        }
    }
}
