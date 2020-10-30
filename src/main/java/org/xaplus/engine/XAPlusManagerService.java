package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusReportTransactionStatusRequestEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionFinishedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
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
        XAPlusTransactionTimedOutEvent.Handler,
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
            if (logger.isInfoEnabled()) {
                logger.info("User start 2pc protocol, {}", transaction);
            }
            dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        }
    }

    @Override
    public void handleUserRollbackRequest(XAPlusUserRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            if (logger.isInfoEnabled()) {
                logger.info("User start rollback protocol, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
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
            dispatcher.dispatch(new XAPlusTransactionFinishedEvent(transaction));
            transaction.getFuture().put(new XAPlusResult(true));
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
            dispatcher.dispatch(new XAPlusTransactionFinishedEvent(transaction));
            transaction.getFuture().put(new XAPlusResult(false));
        }
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            if (event.needRollback()) {
                XAPlusTransaction transaction = tracker.getTransaction(xid);
                if (logger.isInfoEnabled()) {
                    logger.info("Transaction 2pc failed, start rollback protocol, {}", transaction);
                }
                dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
            } else {
                XAPlusTransaction transaction = tracker.remove(xid);
                if (logger.isInfoEnabled()) {
                    logger.info("Transaction 2pc failed, finish transaction, {}", transaction);
                }
                dispatcher.dispatch(new XAPlusTransactionFinishedEvent(transaction));
                transaction.getFuture().put(new XAPlusResult(false));
            }
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        if (transaction != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction rollback failed, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTransactionFinishedEvent(transaction));
            transaction.getFuture().put(new XAPlusResult(new XAPlusRollbackException()));
        }
    }

    @Override
    public void handleTransactionTimedOut(XAPlusTransactionTimedOutEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = tracker.remove(event.getTransaction().getXid());
        if (transaction != null) {
            if (logger.isInfoEnabled()) {
                logger.info("Transaction timed out, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusTransactionFinishedEvent(transaction));
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
            if (logger.isInfoEnabled()) {
                logger.info("Remote superior order to commit, " +
                        "but transaction manager has not such in-flight transaction, xid={}", xid);
            }
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
            if (logger.isInfoEnabled()) {
                logger.info("Remote superior order to rollback, " +
                        "but transaction manager has not such in-flight transaction, xid={}", xid);
            }
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
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToCommitEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }

    private void reportTransactionStatus(XAPlusXid xid) throws InterruptedException {
        String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
        try {
            XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
            if (logger.isDebugEnabled()) {
                logger.debug("Report transaction status to {}, xid={}", superiorServerId, xid);
            }
            dispatcher.dispatch(new XAPlusReportTransactionStatusRequestEvent(xid, resource));
        } catch (XAPlusSystemException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Report transaction status for non XA+ " +
                        "or unknown resource with name={}, xid={}", superiorServerId, xid);
            }
        }
    }
}

