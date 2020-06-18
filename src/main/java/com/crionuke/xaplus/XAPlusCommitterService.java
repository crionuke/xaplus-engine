package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.*;
import com.crionuke.xaplus.events.twopc.XAPlus2pcDoneEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcFailedEvent;
import com.crionuke.xaplus.events.xaplus.XAPlusRemoteSubordinateDoneEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.transaction.xa.XAResource;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusCommitterService extends Bolt implements
        XAPlusTransactionPreparedEvent.Handler,
        XAPlusCommitTransactionEvent.Handler,
        XAPlusCommitTransactionDecisionLoggedEvent.Handler,
        XAPlusCommitTransactionDecisionFailedEvent.Handler,
        XAPlusBranchCommittedEvent.Handler,
        XAPlusCommitBranchFailedEvent.Handler,
        XAPlusRemoteSubordinateDoneEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusCommitterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final State state;

    XAPlusCommitterService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super("committer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        state = new State();
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
        XAPlusXid xid = event.getXid();
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
    public void handleCommitTransactionDecisionFailed(XAPlusCommitTransactionDecisionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
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
        XAPlusTransaction transaction = state.getTransaction(xid);
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
    public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        state.remove(xid);
    }

    @Override
    public void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        state.remove(xid);
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusTransactionPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusCommitTransactionEvent.class);
        dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusCommitTransactionDecisionFailedEvent.class);
        dispatcher.subscribe(this, XAPlusBranchCommittedEvent.class);
        dispatcher.subscribe(this, XAPlusCommitBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
    }

    private void fireCommitDecision(XAPlusTransaction transaction) throws InterruptedException {
        state.track(transaction);
        XAPlusXid xid = transaction.getXid();
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(xid, transaction.getUniqueNames()));
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (state.check(xid)) {
            XAPlusTransaction transaction = state.getTransaction(xid);
            dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        }
    }

    private class State {
        private final Map<XAPlusXid, XAPlusTransaction> transactions;
        private final Map<XAPlusXid, XAPlusXid> branchToTransactionXids;
        private final Map<XAPlusXid, Set<XAPlusXid>> waiting;

        State() {
            transactions = new HashMap<>();
            branchToTransactionXids = new HashMap<>();
            waiting = new HashMap<>();
        }

        void track(XAPlusTransaction transaction) {
            XAPlusXid xid = transaction.getXid();
            transactions.put(xid, transaction);
            HashSet branches = new HashSet();
            transaction.getXaResources().forEach((x, r) -> {
                branches.add(x);
            });
            transaction.getXaPlusResources().forEach((x, r) -> {
                branches.add(x);
                branchToTransactionXids.put(x, xid);
            });
            waiting.put(xid, branches);
        }

        XAPlusTransaction getTransaction(XAPlusXid xid) {
            return transactions.get(xid);
        }

        XAPlusXid getTransactionXid(XAPlusXid branchXid) {
            return branchToTransactionXids.get(branchXid);
        }

        void setCommitted(XAPlusXid xid, XAPlusXid branchXid) {
            Set<XAPlusXid> remaining = waiting.get(xid);
            if (remaining != null) {
                remaining.remove(branchXid);
            }
        }

        void setDone(XAPlusXid branchXid) {
            XAPlusXid xid = branchToTransactionXids.get(branchXid);
            if (xid != null) {
                Set<XAPlusXid> remaining = waiting.get(xid);
                if (remaining != null) {
                    remaining.remove(branchXid);
                }
            }
        }

        void remove(XAPlusXid xid) {
            XAPlusTransaction transaction = transactions.remove(xid);
            transaction.getXaPlusResources().forEach((x, r) -> branchToTransactionXids.remove(x));
            waiting.remove(xid);
        }

        Boolean check(XAPlusXid xid) {
            if (transactions.containsKey(xid) && waiting.containsKey(xid)) {
                return waiting.get(xid).isEmpty();
            } else {
                return false;
            }
        }
    }
}
