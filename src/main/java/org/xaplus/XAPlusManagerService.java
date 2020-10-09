package org.xaplus;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.events.*;
import org.xaplus.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.xaplus.exceptions.XAPlusCommitException;
import org.xaplus.exceptions.XAPlusRollbackException;
import org.xaplus.exceptions.XAPlusTimeoutException;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusManagerService extends Bolt implements
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlusRollbackDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusManagerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final State state;

    XAPlusManagerService(XAPlusProperties XAPlusProperties, XAPlusThreadPool threadPool,
                         XAPlusDispatcher dispatcher) {
        super("manager", XAPlusProperties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        state = new State();
    }

    @Override
    public void handleUserCommitRequest(XAPlusUserCommitRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (state.track(transaction)) {
            dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        }
    }

    @Override
    public void handleUserRollbackRequest(XAPlusUserRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (state.track(transaction)) {
            dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        }
    }

    @Override
    public void handleRemoteSuperiorOrderToRollback(XAPlusRemoteSuperiorOrderToRollbackEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusTransaction transaction = state.getTransaction(xid);
        if (transaction != null && transaction.isSubordinate()) {
            dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        }
    }

    @Override
    public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (state.remove(transaction.getXid()) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("xid={} done", transaction.getXid());
            }
            transaction.getFuture().put(new XAPlusResult());
        }
    }

    @Override
    public void handleRollbackDone(XAPlusRollbackDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        if (state.remove(xid) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("xid={} rolled back", xid);
            }
            transaction.getFuture().put(new XAPlusResult());
        }
    }

    @Override
    public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        Exception exception = event.getException();
        XAPlusTransaction transaction = state.remove(xid);
        if (transaction != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("xid={} 2pc failed, exception=\"{}\"", xid, exception.getMessage());
            }
            transaction.getFuture().put(new XAPlusResult(new XAPlusCommitException(exception)));
        }
    }

    @Override
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        Exception exception = event.getException();
        XAPlusXid xid = event.getTransaction().getXid();
        if (state.remove(xid) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("xid={} rollback failed, exception=\"{}\"", xid, exception.getMessage());
            }
            transaction.getFuture().put(new XAPlusResult(new XAPlusRollbackException(exception)));
        }
    }

    @Override
    public void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = event.getTransaction().getXid();
        if (state.remove(xid) != null) {
            if (logger.isDebugEnabled()) {
                logger.debug("xid={} timed out", xid);
            }
            transaction.getFuture().put(new XAPlusResult(new XAPlusTimeoutException()));
        }
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
    }

    private class State {

        private final Map<XAPlusXid, XAPlusTransaction> transactions;

        State() {
            transactions = new HashMap<>();
        }

        boolean track(XAPlusTransaction transaction) {
            return transactions.put(transaction.getXid(), transaction) == null;
        }

        XAPlusTransaction getTransaction(XAPlusXid xid) {
            return transactions.get(xid);
        }

        XAPlusTransaction remove(XAPlusXid xid) {
            return transactions.remove(xid);
        }
    }
}

