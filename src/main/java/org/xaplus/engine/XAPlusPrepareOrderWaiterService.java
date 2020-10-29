package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.rollback.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.twopc.XAPlusPrepareTransactionEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToPrepareEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusPrepareOrderWaiterService extends Bolt implements
        XAPlus2pcRequestEvent.Handler,
        XAPlusRemoteSuperiorOrderToPrepareEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPrepareOrderWaiterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTracker tracker;

    XAPlusPrepareOrderWaiterService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                    XAPlusDispatcher dispatcher, XAPlusTracker tracker) {
        super(properties.getServerId() + "-prepare-order-waiter", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.tracker = tracker;
    }

    @Override
    public void handle2pcRequest(XAPlus2pcRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSubordinate()) {
            if (tracker.track(transaction)) {
                if (logger.isDebugEnabled()) {
                    String superiorServerId = transaction.getXid().getGlobalTransactionIdUid().extractServerId();
                    logger.debug("Wait order to prepare for transaction from superior, superiorServerId={}, {}",
                            superiorServerId, transaction);
                }
                check(transaction.getXid());
            }
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToPrepare(XAPlusRemoteSuperiorOrderToPrepareEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        tracker.addOrder(xid);
        if (logger.isDebugEnabled()) {
            String superiorServerId = xid.getGlobalTransactionIdUid().extractServerId();
            logger.debug("Got prepare order from superior, superiorServerId={} xid={}",
                    superiorServerId, xid);
        }
        check(xid);
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
                logger.debug("Transaction removed as 2pc failed, {}", transaction);
            }
        }
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
                logger.debug("Transaction removed as timed out, {}", transaction);
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
            XAPlusTransaction transaction = tracker.remove(xid);
            if (logger.isDebugEnabled()) {
                String superiorServerId = transaction.getXid().getGlobalTransactionIdUid().extractServerId();
                logger.debug("2pc protocol cancelled as got order to rollback from superior, superiorServerId={}, {}",
                        superiorServerId, transaction);
            }
            dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToPrepareEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (tracker.contains(xid) && tracker.hasOrder(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            dispatcher.dispatch(new XAPlusPrepareTransactionEvent(transaction));
        }
    }
}
