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
import org.xaplus.engine.events.xaplus.XAPlusReportFailedStatusRequestEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

class XAPlusSubordinateCommitterService extends Bolt implements
        XAPlusCommitTransactionDecisionLoggedEvent.Handler,
        XAPlusLogCommitTransactionDecisionFailedEvent.Handler,
        XAPlusBranchCommittedEvent.Handler,
        XAPlusCommitBranchFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSubordinateCommitterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusSubordinateCommitterService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                     XAPlusResources resources, XAPlusTracker tracker) {
        super(properties.getServerId() + "-subordinate-committer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.tracker = tracker;
        this.resources = resources;
    }

    @Override
    public void handleCommitTransactionDecisionLogged(XAPlusCommitTransactionDecisionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSubordinate() && tracker.track(transaction)) {
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
        if (transaction.isSubordinate()) {
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
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
    }

    void check(XAPlusTransaction transaction) throws InterruptedException {
        if (transaction.isCommitDone()) {
            XAPlusXid xid = transaction.getXid();
            tracker.remove(xid);
            if (transaction.hasFailures()) {
                String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
                try {
                    XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                    dispatcher.dispatch(new XAPlusReportFailedStatusRequestEvent(xid, resource));
                    dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction));
                } catch (XAPlusSystemException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Non XA+ or unknown resource with name={}, {}", superiorServerId, transaction);
                    }
                }
            } else {
                dispatcher.dispatch(new XAPlusLogCompletedTransactionEvent(transaction, true));
            }
        }
    }
}
