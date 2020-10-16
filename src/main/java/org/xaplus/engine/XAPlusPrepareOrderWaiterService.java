package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.XAPlusPrepareTransactionEvent;
import org.xaplus.engine.events.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.XAPlusTimeoutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToPrepareEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusPrepareOrderWaiterService extends Bolt implements
        XAPlus2pcRequestEvent.Handler,
        XAPlusRemoteSuperiorOrderToPrepareEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPrepareOrderWaiterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusPrepareOrderWaiterState state;

    XAPlusPrepareOrderWaiterService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                    XAPlusDispatcher dispatcher) {
        super("prepare-order-waiter", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.state = new XAPlusPrepareOrderWaiterState();
    }

    @Override
    public void handle2pcRequest(XAPlus2pcRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSubordinate()) {
            if (state.track(transaction)) {
                XAPlusXid xid = event.getTransaction().getXid();
                check(xid);
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction {} is not subordinate, skip", transaction);
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
        state.addPrepareOrder(xid);
        check(xid);
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        XAPlusTransaction transaction = state.remove(xid);
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
        XAPlusTransaction transaction = state.remove(xid);
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
        XAPlusTransaction transaction = state.remove(xid);
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
        dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToPrepareEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (state.check(xid)) {
            XAPlusTransaction transaction = state.remove(xid);
            dispatcher.dispatch(new XAPlusPrepareTransactionEvent(transaction));
        }
    }
}
