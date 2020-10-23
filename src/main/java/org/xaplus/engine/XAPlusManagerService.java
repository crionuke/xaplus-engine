package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;
import org.xaplus.engine.events.journal.XAPlusReportTransactionStatusRequestEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusSystemException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusManagerService extends Bolt implements
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlusRollbackDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler,
        XAPlusRemoteSuperiorOrderToCommitEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusManagerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusManagerService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                         XAPlusDispatcher dispatcher, XAPlusResources resources) {
        super(properties.getServerId() + "-manager", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        tracker = new XAPlusTracker();
    }

    @Override
    public void handleUserCommitRequest(XAPlusUserCommitRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction already tracked, {}", transaction);
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
            dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction already tracked, {}", transaction);
            }
        }
    }

    @Override
    public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        XAPlusTransaction transaction = tracker.remove(xid);
        if (transaction != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction done, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTransactionCompletedEvent(transaction));
            transaction.getFuture().put(new XAPlusResult());
        }
    }

    @Override
    public void handleRollbackDone(XAPlusRollbackDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        if (transaction != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction rolled back, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTransactionCompletedEvent(transaction));
            transaction.getFuture().put(new XAPlusResult());
        }
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        Exception exception = event.getException();
        if (transaction != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction 2pc failed as {}, {}", exception.getMessage(), transaction);
            }
            dispatcher.dispatch(new XAPlusTransactionCompletedEvent(transaction));
            transaction.getFuture().put(new XAPlusResult(new XAPlusCommitException(exception)));
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        Exception exception = event.getException();
        if (transaction != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction rollback failed as {}, {}", exception.getMessage(), transaction);
            }
            dispatcher.dispatch(new XAPlusTransactionCompletedEvent(transaction));
            transaction.getFuture().put(new XAPlusResult(new XAPlusRollbackException(exception)));
        }
    }

    @Override
    public void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        if (transaction != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction timed out, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTransactionCompletedEvent(transaction));
            transaction.getFuture().put(new XAPlusResult(new XAPlusTimeoutException()));
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToCommit(XAPlusRemoteSuperiorOrderToCommitEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.getTransaction(xid) == null) {
            reportTransactionStatus(xid);
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToRollback(XAPlusRemoteSuperiorOrderToRollbackEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.getTransaction(xid) == null) {
            reportTransactionStatus(xid);
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToCommitEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }

    private void reportTransactionStatus(XAPlusXid xid) throws InterruptedException {
        String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
        try {
            XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
            if (logger.isTraceEnabled()) {
                logger.trace("Report transaction status to {}, xid={}", superiorServerId, xid);
            }
            dispatcher.dispatch(new XAPlusReportTransactionStatusRequestEvent(xid, resource));
        } catch (XAPlusSystemException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Internal error. Report transaction status for non XA+ " +
                        "or unknown xaResource with name={}", superiorServerId);
            }
        }
    }
}

