package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackTransactionDecisionEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCreateTransactionEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xa.XAPlusBranchPreparedEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateReadyEvent;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusSuperiorPreparerService extends Bolt implements
        XAPlusUserCreateTransactionEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlusRemoteSubordinateReadyEvent.Handler,
        XAPlusRemoteSubordinateFailedEvent.Handler,
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusBranchPreparedEvent.Handler,
        XAPlusPrepareBranchFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSuperiorPreparerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusSuperiorPreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                  XAPlusDispatcher dispatcher, XAPlusResources resources, XAPlusTracker tracker) {
        super(properties.getServerId() + "-superior-preparer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.tracker = tracker;
        this.resources = resources;
    }

    @Override
    public void handleUserCreateTransaction(XAPlusUserCreateTransactionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior() && tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("User create superior side transaction, {}", transaction);
            }
        }
    }

    @Override
    public void handleUserRollbackRequest(XAPlusUserRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Log rollback decision, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        }
    }

    @Override
    public void handleRemoteSubordinateReady(XAPlusRemoteSubordinateReadyEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getXid();
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            if (logger.isDebugEnabled()) {
                String subordinateServerId = branchXid.getBranchQualifierUid().extractServerId();
                logger.debug("Remote branch ready, subordinateServerId={}, {}",
                        subordinateServerId, transaction);
            }
            transaction.branchPrepared(branchXid);
            check(transaction);
        }
    }

    @Override
    public void handleRemoteSubordinateFailed(XAPlusRemoteSubordinateFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getXid();
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            if (logger.isDebugEnabled()) {
                String subordinateServerId = branchXid.getBranchQualifierUid().extractServerId();
                logger.debug("Remote branch failed, subordinateServerId={}, {}",
                        subordinateServerId, transaction);
            }
            transaction.branchPrepared(branchXid);
            transaction.branchFailed(branchXid);
            check(transaction);
        }
    }

    @Override
    public void handleUserCommitRequest(XAPlusUserCommitRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Prepare transaction, {}", transaction);
            }
            transaction.prepare(dispatcher);
        }
    }

    @Override
    public void handleBranchPrepared(XAPlusBranchPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getBranchXid();
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch prepared, xid={}, {}", branchXid, transaction);
            }
            transaction.branchPrepared(branchXid);
            check(transaction);
        }
    }

    @Override
    public void handlePrepareBranchFailed(XAPlusPrepareBranchFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getBranchXid();
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Prepare branch failed, xid={}, {}", branchXid, transaction);
            }
            transaction.branchPrepared(branchXid);
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
        dispatcher.subscribe(this, XAPlusUserCreateTransactionEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateReadyEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateFailedEvent.class);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusBranchPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusPrepareBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
    }

    void check(XAPlusTransaction transaction) throws InterruptedException {
        if (transaction.isPrepareDone()) {
            tracker.remove(transaction.getXid());
            if (transaction.hasFailures()) {
                dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
            } else {
                dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
            }
        }
    }
}

