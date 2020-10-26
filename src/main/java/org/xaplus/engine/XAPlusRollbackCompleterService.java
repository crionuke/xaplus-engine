package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.journal.XAPlusCompletedTransactionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogCompletedTransactionEvent;
import org.xaplus.engine.events.journal.XAPlusLogComplettedTransactionFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusTransactionRolledBackEvent;
import org.xaplus.engine.events.xaplus.XAPlusDoneStatusReportedEvent;
import org.xaplus.engine.events.xaplus.XAPlusReportDoneStatusRequestEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;

class XAPlusRollbackCompleterService extends Bolt implements
        XAPlusTransactionRolledBackEvent.Handler,
        XAPlusDoneStatusReportedEvent.Handler,
        XAPlusCompletedTransactionLoggedEvent.Handler,
        XAPlusLogComplettedTransactionFailedEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRollbackCompleterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusRollbackCompleterService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                                   XAPlusResources resources, XAPlusTracker tracker) {
        super(properties.getServerId() + "-rollback-completer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
        this.tracker = tracker;
    }

    @Override
    public void handleTransactionRolledBack(XAPlusTransactionRolledBackEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            if (transaction.isSuperior()) {
                dispatcher.dispatch(new XAPlusLogCompletedTransactionEvent(transaction, false));
            } else {
                // Report done from subordinate to superior
                XAPlusXid xid = transaction.getXid();
                String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
                try {
                    XAPlusResource resource = resources.getXAPlusResource(superiorServerId);
                    dispatcher.dispatch(new XAPlusReportDoneStatusRequestEvent(xid, resource));
                } catch (XAPlusSystemException doneException) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("Subordinate transaction failed as {}, {}",
                                doneException.getMessage(), transaction);
                    }
                    dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction, doneException));
                }
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction already tracked, {}", transaction);
            }
        }
    }

    @Override
    public void handleDoneStatusReported(XAPlusDoneStatusReportedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            dispatcher.dispatch(new XAPlusLogCompletedTransactionEvent(transaction, true));
        }
    }

    @Override
    public void handleCompletedTransactionLogged(XAPlusCompletedTransactionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
        }
    }

    @Override
    public void handleLogCompletedTransactionFailed(XAPlusLogComplettedTransactionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction, event.getException()));
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        tracker.remove(xid);
    }

    @Override
    public void handleTransactionTimedOut(XAPlusTransactionTimedOutEvent event) {
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
        dispatcher.subscribe(this, XAPlusTransactionRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusDoneStatusReportedEvent.class);
        dispatcher.subscribe(this, XAPlusCompletedTransactionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusLogComplettedTransactionFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
    }
}
