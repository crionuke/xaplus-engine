package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackTransactionDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackTransactionDecisionFailedEvent;
import org.xaplus.engine.events.journal.XAPlusRollbackTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.rollback.XAPlusTransactionRolledBackEvent;
import org.xaplus.engine.events.xa.XAPlusBranchRolledBackEvent;
import org.xaplus.engine.events.xa.XAPlusRollbackBranchFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateDoneEvent;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusRollbackService extends Bolt implements
        XAPlusRollbackRequestEvent.Handler,
        XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
        XAPlusLogRollbackTransactionDecisionFailedEvent.Handler,
        XAPlusBranchRolledBackEvent.Handler,
        XAPlusRollbackBranchFailedEvent.Handler,
        XAPlusRemoteSubordinateDoneEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRollbackService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTracker tracker;

    XAPlusRollbackService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                          XAPlusTracker tracker) {
        super(properties.getServerId() + "-rollback", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.tracker = tracker;
    }

    @Override
    public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction already tracked, {}", transaction);
            }
        }
    }

    @Override
    public void handleRollbackTransactionDecisionLogged(XAPlusRollbackTransactionDecisionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            tracker.getTransaction(xid).rollback(dispatcher);
        }
    }

    @Override
    public void handleLogRollbackTransactionDecisionFailed(XAPlusLogRollbackTransactionDecisionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            Exception exception = event.getException();
            dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction, exception));
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
            transaction.branchRolledBack(branchXid);
            check(xid);
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction not found, xid={}", xid);
            }
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
            Exception exception = event.getException();
            dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction, exception));
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction not found, xid={}", xid);
            }
        }
    }

    @Override
    public void handleRemoteSubordinateDone(XAPlusRemoteSubordinateDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getXid();
        XAPlusXid xid = tracker.getTransactionXid(branchXid);
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            transaction.branchDone(branchXid);
            check(xid);
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        XAPlusTransaction transaction = tracker.remove(xid);
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction removed, {}", transaction);
            }
        }
    }

    @Override
    public void handleTransactionTimedOut(XAPlusTransactionTimedOutEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        XAPlusTransaction transaction = tracker.remove(xid);
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction removed, {}", transaction);
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionFailedEvent.class);
        dispatcher.subscribe(this, XAPlusBranchRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            if (transaction.isRolledBack() && transaction.isDone()) {
                tracker.remove(xid);
                dispatcher.dispatch(new XAPlusTransactionRolledBackEvent(transaction));
            }
        }
    }
}