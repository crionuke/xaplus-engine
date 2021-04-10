package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlusRollbackTransactionDecisionEvent;
import org.xaplus.engine.events.xa.XAPlusBranchRolledBackEvent;
import org.xaplus.engine.events.xa.XAPlusRollbackBranchFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusReportFailedStatusRequestEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusSubordinateRollbackService extends Bolt implements
        XAPlusRollbackTransactionDecisionEvent.Handler,
        XAPlusBranchRolledBackEvent.Handler,
        XAPlusRollbackBranchFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSubordinateRollbackService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusSubordinateRollbackService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                     XAPlusResources resources, XAPlusTracker tracker) {
        super(properties.getServerId() + "-rollback", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = tracker;
    }

    @Override
    public void handleRollbackTransactionDecision(XAPlusRollbackTransactionDecisionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSubordinate() && tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback transaction, {}", transaction);
            }
            transaction.reset();
            transaction.rollback(dispatcher);
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
        dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusBranchRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
    }

    void check(XAPlusTransaction transaction) throws InterruptedException {
        if (transaction.isRollbackDone()) {
            XAPlusXid xid = transaction.getXid();
            tracker.remove(xid);
            String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
            try {
                XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                if (transaction.hasFailures()) {
                    dispatcher.dispatch(new XAPlusReportFailedStatusRequestEvent(xid, resource));
                    dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction));
                } else {
                    dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
                }
            } catch (XAPlusSystemException e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Non XA+ or unknown resource with name={}, {}", superiorServerId, transaction);
                }
            }
        }
    }
}
