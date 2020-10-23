package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusPrepareTransactionEvent;
import org.xaplus.engine.events.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.XAPlusTimeoutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
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
        XAPlusTimeoutEvent.Handler,
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
                check(transaction.getXid());
            } else {
                if (logger.isTraceEnabled()) {
                    logger.trace("Transaction already tracked, {}", transaction);
                }
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction is not subordinate, {}", transaction);
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
        check(xid);
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("2pc protocol cancelled as transaction failed, {}", transaction);
            }
        }
    }

    @Override
    public void handleTimeout(XAPlusTimeoutEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("2pc protocol cancelled as transaction timed out, {}", transaction);
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
                logger.debug("2pc protocol cancelled as got order to rollback, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToPrepareEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (tracker.contains(xid) && tracker.hasOrder(xid)) {
            XAPlusTransaction transaction = tracker.remove(xid);
            dispatcher.dispatch(new XAPlusPrepareTransactionEvent(transaction));
        }
    }
}
