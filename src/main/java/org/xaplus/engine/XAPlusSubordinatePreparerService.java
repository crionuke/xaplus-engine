package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusCommitTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusRollbackTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCreateTransactionEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xa.XAPlusBranchPreparedEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchFailedEvent;
import org.xaplus.engine.events.xaplus.*;
import org.xaplus.engine.exceptions.XAPlusSystemException;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusSubordinatePreparerService extends Bolt implements
        XAPlusUserCreateTransactionEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler,
        XAPlusRemoteSuperiorOrderToCommitEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusBranchPreparedEvent.Handler,
        XAPlusPrepareBranchFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler,
        XAPlusReportReadyStatusFailedEvent.Handler,
        XAPlusReportFailedStatusFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSubordinatePreparerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusSubordinatePreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                     XAPlusDispatcher dispatcher, XAPlusResources resources) {
        super(properties.getServerId() + "-subordinate-preparer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = new XAPlusTracker();
    }

    @Override
    public void handleUserCreateTransaction(XAPlusUserCreateTransactionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSubordinate() && tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("User create subordinate side transaction, {}", transaction);
            }
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToRollback(XAPlusRemoteSuperiorOrderToRollbackEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            // Or preparation finished, or transaction already marked as rollback only by user rollback request
            if (transaction.isPrepared() || transaction.isDecided() && transaction.isRollbackOnly()) {
                tracker.remove(xid);
                if (logger.isDebugEnabled()) {
                    logger.debug("Rollback decision, {}", transaction);
                }
                // Start rollback on subordinate side without logging
                dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(transaction));
            } else {
                // Just mark as rollback only, wait when preparation finished
                transaction.markAsRollbackOnly();
            }
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToCommit(XAPlusRemoteSuperiorOrderToCommitEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            if (transaction.isPrepared()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Commit decision, {}", transaction);
                }
                // Start commit on subordinate side without logging
                dispatcher.dispatch(new XAPlusCommitTransactionDecisionLoggedEvent(transaction));
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("Remote superior order to commit, but transaction not prepared yet, xid={}", xid);
                }
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
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            // If rollback request already received
            if (transaction.isRollbackOnly()) {
                tracker.remove(xid);
                if (logger.isDebugEnabled()) {
                    logger.debug("Rollback decision, {}", transaction);
                }
                // Skip logging on subordinate side
                dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(transaction));
            } else {
                transaction.markAsDecided();
                transaction.markAsRollbackOnly();
                // Send branch failed event to superior
                String superiorServerId = xid.getGtrid().getServerId();
                try {
                    XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Report failed status, superiorServerId={}, {}", superiorServerId, transaction);
                    }
                    dispatcher.dispatch(new XAPlusReportFailedStatusRequestEvent(xid, resource));
                } catch (XAPlusSystemException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Non XA+ or unknown resource with name={}, {}", superiorServerId, transaction);
                    }
                }
            }
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
            transaction.markAsDecided();
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

    // TODO: if report failed - rollback changes

    @Override
    public void handleReportReadyStatusFailed(XAPlusReportReadyStatusFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction removed as report ready status failed, {}", transaction);
            }
            dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction));
        }
    }

    @Override
    public void handleReportFailedStatusFailed(XAPlusReportFailedStatusFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction removed as report failed status failed, {}", transaction);
            }
            dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction));
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusUserCreateTransactionEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToCommitEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusBranchPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusPrepareBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
        dispatcher.subscribe(this, XAPlusReportReadyStatusFailedEvent.class);
        dispatcher.subscribe(this, XAPlusReportFailedStatusFailedEvent.class);
    }

    void check(XAPlusTransaction transaction) throws InterruptedException {
        if (transaction.isDecided() && transaction.isPrepared()) {
            XAPlusXid xid = transaction.getXid();
            // If rollback request received from superior
            if (transaction.isRollbackOnly()) {
                tracker.remove(xid);
                if (logger.isDebugEnabled()) {
                    logger.debug("Rollback decision, {}", transaction);
                }
                // Skip logging on subordinate side
                dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(transaction));
            } else {
                String superiorServerId = xid.getGtrid().getServerId();
                try {
                    XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                    if (transaction.hasFailures()) {
                        // If some branches failed
                        dispatcher.dispatch(new XAPlusReportFailedStatusRequestEvent(xid, resource));
                    } else {
                        // All is okay, transaction ready to commit or rollback
                        dispatcher.dispatch(new XAPlusReportReadyStatusRequestEvent(xid, resource));
                    }
                } catch (XAPlusSystemException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Non XA+ or unknown resource with name={}, {}", superiorServerId, transaction);
                    }
                }
            }
        }
    }
}
