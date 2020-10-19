package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.*;
import org.xaplus.engine.events.journal.XAPlusReportTransactionStatusRequestEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

import javax.annotation.PostConstruct;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusCommitOrderWaiterService extends Bolt implements
        XAPlusTransactionPreparedEvent.Handler,
        XAPlusRemoteSuperiorOrderToCommitEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusCommitOrderWaiterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusCommitOrderWaiterService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                   XAPlusDispatcher dispatcher, XAPlusResources resources) {
        super("commit-order-waiter", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        tracker = new XAPlusTracker();
    }

    @Override
    public void handleTransactionPrepared(XAPlusTransactionPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSubordinate()) {
            if (tracker.track(transaction)) {
                XAPlusXid xid = transaction.getXid();
                String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
                try {
                    XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                    dispatcher.dispatch(new XAPlusReportReadyStatusRequestEvent(xid, resource));
                } catch (XAPlusSystemException readyException) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Subordinate transaction with xid={} failed as {}",
                                transaction.getXid(), readyException.getMessage());
                    }
                    dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction, readyException));
                }
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction {} is not subordinate, skip", transaction);
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
        XAPlusTransaction transaction = tracker.remove(xid);
        if (transaction != null) {
            dispatcher.dispatch(new XAPlusCommitTransactionEvent(transaction));
        } else {
            String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
            try {
                XAPlusResource xaPlusResource = resources.getXAPlusResource(superiorServerId);
                dispatcher.dispatch(new XAPlusReportTransactionStatusRequestEvent(xaPlusResource, xid));
            } catch (XAPlusSystemException e) {
                if (logger.isErrorEnabled()) {
                    logger.error("Internal error. Report transaction status for non XA+ " +
                            "or unknown resource with name={}", superiorServerId);
                }
            }
        }
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        XAPlusTransaction transaction = tracker.remove(xid);
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("2pc protocol cancelled for xid={} as transaction failed", xid);
            }
        }
    }

    @Override
    public void handleTimeout(XAPlusTimeoutEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        XAPlusTransaction transaction = tracker.remove(xid);
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("2pc protocol cancelled for xid={} as transaction timed out", xid);
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
        XAPlusTransaction transaction = tracker.remove(xid);
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("2pc protocol cancelled for xid={} as got order to rollback", xid);
            }
            dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        }
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusTransactionPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToCommitEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }
}
