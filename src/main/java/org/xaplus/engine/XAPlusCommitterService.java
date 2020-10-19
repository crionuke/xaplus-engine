package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.XAPlusCommitTransactionEvent;
import org.xaplus.engine.events.XAPlusTimeoutEvent;
import org.xaplus.engine.events.XAPlusTransactionPreparedEvent;
import org.xaplus.engine.events.journal.XAPlusCommitTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.xa.XAPlusBranchCommittedEvent;
import org.xaplus.engine.events.xa.XAPlusCommitBranchFailedEvent;
import org.xaplus.engine.events.xa.XAPlusCommitBranchRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateDoneEvent;

import javax.annotation.PostConstruct;
import javax.transaction.xa.XAResource;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusCommitterService extends Bolt implements
        XAPlusTransactionPreparedEvent.Handler,
        XAPlusCommitTransactionEvent.Handler,
        XAPlusCommitTransactionDecisionLoggedEvent.Handler,
        XAPlusLogCommitTransactionDecisionFailedEvent.Handler,
        XAPlusBranchCommittedEvent.Handler,
        XAPlusCommitBranchFailedEvent.Handler,
        XAPlusRemoteSubordinateDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusCommitterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusCommitterState state;

    XAPlusCommitterService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super("committer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        state = new XAPlusCommitterState();
    }

    @Override
    public void handleTransactionPrepared(XAPlusTransactionPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior()) {
            fireCommitDecision(transaction);
        }
    }

    @Override
    public void handleCommitTransaction(XAPlusCommitTransactionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        fireCommitDecision(transaction);
    }

    @Override
    public void handleCommitTransactionDecisionLogged(XAPlusCommitTransactionDecisionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        XAPlusTransaction transaction = state.getTransaction(xid);
        if (transaction != null) {
            Map<XAPlusXid, XAResource> resources = new HashMap<>();
            resources.putAll(transaction.getXaResources());
            resources.putAll(transaction.getXaPlusResources());
            for (Map.Entry<XAPlusXid, XAResource> entry : resources.entrySet()) {
                XAPlusXid branchXid = entry.getKey();
                XAResource resource = entry.getValue();
                dispatcher.dispatch(new XAPlusCommitBranchRequestEvent(xid, branchXid, resource));
            }
        }
    }

    @Override
    public void handleLogCommitTransactionDecisionFailed(XAPlusLogCommitTransactionDecisionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        XAPlusTransaction transaction = state.getTransaction(xid);
        if (transaction != null) {
            Exception exception = event.getException();
            dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction, exception));
        }
    }

    @Override
    public void handleBranchCommitted(XAPlusBranchCommittedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusXid branchXid = event.getBranchXid();
        state.setCommitted(xid, branchXid);
        check(xid);
    }

    @Override
    public void handleCommitBranchFailed(XAPlusCommitBranchFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusTransaction transaction = state.remove(xid);
        if (transaction != null) {
            Exception exception = event.getException();
            dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction, exception));
        }
    }

    @Override
    public void handleRemoteSubordinateDone(XAPlusRemoteSubordinateDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getXid();
        state.setDone(branchXid);
        XAPlusXid xid = state.getTransactionXid(branchXid);
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

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusTransactionPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusCommitTransactionEvent.class);
        dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionFailedEvent.class);
        dispatcher.subscribe(this, XAPlusBranchCommittedEvent.class);
        dispatcher.subscribe(this, XAPlusCommitBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
    }

    private void fireCommitDecision(XAPlusTransaction transaction) throws InterruptedException {
        if (state.track(transaction)) {
            dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
        }
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (state.check(xid)) {
            XAPlusTransaction transaction = state.remove(xid);
            dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        }
    }
}
