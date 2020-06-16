package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.*;
import com.crionuke.xaplus.events.twopc.XAPlus2pcDoneEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcFailedEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcRequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
final class XAPlusTimerService extends Bolt implements
        XAPlus2pcRequestEvent.Handler,
        XAPlusRollbackRequestEvent.Handler,
        XAPlusTickEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlusRollbackDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusRollbackFailedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTimerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final State state;

    XAPlusTimerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super("xaplus-timer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        state = new State();
    }

    @Override
    public void handle2pcRequest(XAPlus2pcRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        state.track(transaction);
        if (logger.isDebugEnabled()) {
            logger.trace("Track timeout for xid={}", transaction.getXid());
        }
    }

    @Override
    public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        state.track(transaction);
        if (logger.isDebugEnabled()) {
            logger.trace("Track timeout for xid={}", transaction.getXid());
        }
    }

    @Override
    public void handleTick(XAPlusTickEvent event) throws InterruptedException {
        List<XAPlusXid> expiredTransaction = state.removeExpiredTransactions();
        for (XAPlusXid xid : expiredTransaction) {
            dispatcher.dispatch(new XAPlusTimeoutEvent(xid));
        }
    }

    @Override
    public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (state.remove(transaction.getXid())) {
            if (logger.isDebugEnabled()) {
                logger.trace("Timeout tracking for xid={} cancelled as 2pc procotol done", transaction.getXid());
            }
        }
    }

    @Override
    public void handleRollbackDone(XAPlusRollbackDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        state.remove(xid);
        if (logger.isDebugEnabled()) {
            logger.trace("Timeout tracking for xid={} cancelled as rollback protocol done", xid);
        }
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (state.remove(transaction.getXid())) {
            if (logger.isDebugEnabled()) {
                logger.trace("Timeout tracking for xid={} cancelled as 2pc protocol failed", transaction.getXid());
            }
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        state.remove(xid);
        if (logger.isDebugEnabled()) {
            logger.trace("Timeout tracking for xid={} cancelled as rollback protocol failed", xid);
        }
    }

    // TODO: fire XAPlusRollbackTimeoutEvent

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusTickEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
    }

    private final class State {

        private final Map<XAPlusXid, XAPlusTransaction> transactions;

        State() {
            transactions = new HashMap<>();
        }

        void track(XAPlusTransaction transaction) {
            transactions.put(transaction.getXid(), transaction);
        }

        boolean remove(XAPlusXid xid) {
            return transactions.remove(xid) != null;
        }

        List<XAPlusXid> removeExpiredTransactions() {
            List<XAPlusXid> expired = new ArrayList<>();
            long time = System.currentTimeMillis();
            Iterator<Map.Entry<XAPlusXid, XAPlusTransaction>> iterator = transactions.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<XAPlusXid, XAPlusTransaction> entry = iterator.next();
                XAPlusXid xid = entry.getKey();
                XAPlusTransaction transaction = entry.getValue();
                if (time >= transaction.getExpireTimeInMillis()) {
                    expired.add(xid);
                    iterator.remove();
                }
            }
            return expired;
        }
    }
}