package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusReportTransactionStatusRequestEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlusCommitTransactionDecisionEvent;
import org.xaplus.engine.events.twopc.XAPlusRollbackTransactionDecisionEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCreateTransactionEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xa.XAPlusBranchPreparedEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchFailedEvent;
import org.xaplus.engine.events.xaplus.*;
import org.xaplus.engine.exceptions.XAPlusSystemException;

class XAPlusSubordinatePreparerService extends Bolt implements
        XAPlusUserCreateTransactionEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler,
        XAPlusRemoteSuperiorOrderToCommitEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusBranchPreparedEvent.Handler,
        XAPlusPrepareBranchFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler,
        XAPlusReportReadiedStatusFailedEvent.Handler,
        XAPlusReportFailedStatusFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSubordinatePreparerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusSubordinatePreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                     XAPlusDispatcher dispatcher, XAPlusResources resources, XAPlusTracker tracker) {
        super(properties.getServerId() + "-subordinate-preparer", properties.getQueueSize());
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
            if (transaction.isPrepareDone() || transaction.isRollbackOnly()) {
                tracker.remove(xid);
                if (logger.isDebugEnabled()) {
                    logger.debug("Rollback decision, {}", transaction);
                }
                dispatcher.dispatch(new XAPlusRollbackTransactionDecisionEvent(transaction));
            } else {
                // Just mark as rollback only, wait when preparation finished
                transaction.markAsRollbackOnly();
            }
        } else {
            reportTransactionStatus(xid);
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
            if (transaction.isPrepareDone()) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Commit decision, {}", transaction);
                }
                dispatcher.dispatch(new XAPlusCommitTransactionDecisionEvent(transaction));
            } else {
                if (logger.isWarnEnabled()) {
                    logger.warn("Remote superior order to commit, but transaction not prepared yet, xid={}", xid);
                }
            }
        } else {
            reportTransactionStatus(xid);
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
                dispatcher.dispatch(new XAPlusRollbackTransactionDecisionEvent(transaction));
            } else {
                transaction.markAsRollbackOnly();
                // Send branch cancelled event to superior
                String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
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

    @Override
    public void handleReportReadiedStatusFailed(XAPlusReportReadiedStatusFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction removed as report readied status failed, {}", transaction);
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
        dispatcher.subscribe(this, XAPlusReportReadiedStatusFailedEvent.class);
        dispatcher.subscribe(this, XAPlusReportFailedStatusFailedEvent.class);
    }

    void check(XAPlusTransaction transaction) throws InterruptedException {
        if (transaction.isPrepareDone()) {
            XAPlusXid xid = transaction.getXid();
            // If rollback request received from superior
            if (transaction.isRollbackOnly()) {
                tracker.remove(xid);
                if (logger.isDebugEnabled()) {
                    logger.debug("Rollback decision, {}", transaction);
                }
                dispatcher.dispatch(new XAPlusRollbackTransactionDecisionEvent(transaction));
            } else {
                String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
                try {
                    XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                    if (transaction.hasFailures()) {
                        // If some branches failed
                        dispatcher.dispatch(new XAPlusReportFailedStatusRequestEvent(xid, resource));
                    } else {
                        // All is okay, transaction ready to commit or rollback
                        dispatcher.dispatch(new XAPlusReportReadiedStatusRequestEvent(xid, resource));
                    }
                } catch (XAPlusSystemException e) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Non XA+ or unknown resource with name={}, {}", superiorServerId, transaction);
                    }
                }
            }
        }
    }

    void reportTransactionStatus(XAPlusXid xid) throws InterruptedException {
        String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
        try {
            XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
            if (logger.isDebugEnabled()) {
                logger.debug("Report transaction status, superiorServerId={}, xid={}", superiorServerId, xid);
            }
            dispatcher.dispatch(new XAPlusReportTransactionStatusRequestEvent(xid, resource));
        } catch (XAPlusSystemException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Non XA+ or unknown resource with name={}, xid={}", superiorServerId, xid);
            }
        }
    }
}
