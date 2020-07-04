package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.XAPlusPrepareTransactionEvent;
import com.crionuke.xaplus.events.XAPlusTimeoutEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcFailedEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcRequestEvent;
import com.crionuke.xaplus.events.xaplus.XAPlusRemoteSuperiorOrderToPrepareEvent;
import com.crionuke.xaplus.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

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
    private final State state;

    XAPlusPrepareOrderWaiterService(XAPlusProperties properties, XAPlusThreadPool threadPool,
                                    XAPlusDispatcher dispatcher) {
        super("prepare-order-waiter", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.state = new State();
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
    public void handleRemoteSuperiorOrderToRollback(XAPlusRemoteSuperiorOrderToRollbackEvent event) {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusTransaction transaction = state.remove(xid);
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("2pc protocol cancelled for xid={} as got order to rollback", xid);
            }
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

    private class State {
        private final Map<XAPlusXid, XAPlusTransaction> transactions;
        private final Set<XAPlusXid> prepareOrders;

        State() {
            transactions = new HashMap<>();
            prepareOrders = new HashSet<>();
        }

        boolean track(XAPlusTransaction transaction) {
            return transactions.put(transaction.getXid(), transaction) == null;
        }

        void addPrepareOrder(XAPlusXid xid) {
            prepareOrders.add(xid);
        }

        XAPlusTransaction getTransaction(XAPlusXid xid) {
            return transactions.get(xid);
        }

        XAPlusTransaction remove(XAPlusXid xid) {
            XAPlusTransaction transaction = transactions.remove(xid);
            prepareOrders.remove(xid);
            return transaction;
        }

        Boolean check(XAPlusXid xid) {
            return prepareOrders.contains(xid) && transactions.containsKey(xid);
        }
    }
}
