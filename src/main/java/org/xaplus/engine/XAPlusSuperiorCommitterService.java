package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusCommitTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionFailedEvent;
import org.xaplus.engine.events.journal.XAPlusLogCompletedTransactionEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackTransactionDecisionEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.xa.XAPlusBranchCommittedEvent;
import org.xaplus.engine.events.xa.XAPlusCommitBranchFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateDoneEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateFailedEvent;

class XAPlusSuperiorCommitterService extends Bolt implements
        XAPlusCommitTransactionDecisionLoggedEvent.Handler,
        XAPlusLogCommitTransactionDecisionFailedEvent.Handler,
        XAPlusBranchCommittedEvent.Handler,
        XAPlusCommitBranchFailedEvent.Handler,
        XAPlusRemoteSubordinateDoneEvent.Handler,
        XAPlusRemoteSubordinateFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSuperiorCommitterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusSuperiorCommitterService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                              XAPlusResources resources, XAPlusTracker tracker) {
        super(properties.getServerId() + "-superior-committer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = tracker;
    }

    @Override
    public void handleCommitTransactionDecisionLogged(XAPlusCommitTransactionDecisionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior() && tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Commit transaction, {}", transaction);
            }
            transaction.reset();
            transaction.commit(dispatcher);
        }
    }

    @Override
    public void handleLogCommitTransactionDecisionFailed(XAPlusLogCommitTransactionDecisionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior()) {
            if (logger.isDebugEnabled()) {
                logger.debug("Log rollback decision, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        }
    }

    @Override
    public void handleBranchCommitted(XAPlusBranchCommittedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getBranchXid();
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch committed, xid={}, {}", branchXid, transaction);
            }
            transaction.branchCommitted(branchXid);
            check(transaction);
        }
    }

    @Override
    public void handleCommitBranchFailed(XAPlusCommitBranchFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getBranchXid();
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Commit branch failed, xid={}, {}", branchXid, transaction);
            }
            transaction.branchCommitted(branchXid);
            transaction.branchFailed(branchXid);
            check(transaction);
        }
    }

    @Override
    public void handleRemoteSubordinateDone(XAPlusRemoteSubordinateDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getXid();
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            if (logger.isDebugEnabled()) {
                String subordinateServerId = branchXid.getGlobalTransactionIdUid().extractServerId();
                logger.debug("Remote branch done, subordinateServerId={}, xid={}, {}",
                        subordinateServerId, branchXid, transaction);
            }
            transaction.branchCommitted(branchXid);
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
                String subordinateServerId = branchXid.getGlobalTransactionIdUid().extractServerId();
                logger.debug("Remote branch failed, subordinateServerId={}, xid={}, {}",
                        subordinateServerId, branchXid, transaction);
            }
            transaction.branchCommitted(branchXid);
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
        dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionFailedEvent.class);
        dispatcher.subscribe(this, XAPlusBranchCommittedEvent.class);
        dispatcher.subscribe(this, XAPlusCommitBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
    }

    void check(XAPlusTransaction transaction) throws InterruptedException {
        if (transaction.isCommitDone()) {
            tracker.remove(transaction.getXid());
            if (transaction.hasFailures()) {
                dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction));
            } else {
                dispatcher.dispatch(new XAPlusLogCompletedTransactionEvent(transaction, true));
            }
        }
    }
}
