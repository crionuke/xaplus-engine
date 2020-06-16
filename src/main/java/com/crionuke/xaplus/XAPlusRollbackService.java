package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.*;
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
@Service final class XAPlusRollbackService extends Bolt implements
        XAPlusRollbackRequestEvent.Handler,
        XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
        XAPlusRollbackTransactionDecisionFailedEvent.Handler,
        XAPlusBranchRolledBackEvent.Handler,
        XAPlusRollbackBranchFailedEvent.Handler,
        XAPlusRemoteSubordinateDoneEvent.Handler,
        XAPlusRollbackFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRollbackService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTracker tracker;
    private final State state;

    XAPlusRollbackService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super("xaplus-rollback", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        tracker = new XAPlusTracker();
        state = new State();
    }

    @Override
    public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        state.track(transaction);
        XAPlusXid xid = transaction.getXid();
        dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(xid, transaction.getUniqueNames()));
    }

    @Override
    public void handleRollbackTransactionDecisionLogged(XAPlusRollbackTransactionDecisionLoggedEvent event)
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
                dispatcher.dispatch(new XAPlusRollbackBranchRequestEvent(xid, branchXid, resource));
            }
        }
    }

    @Override
    public void handleRollbackTransactionDecisionFailed(XAPlusRollbackTransactionDecisionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusTransaction transaction = state.getTransaction(xid);
        if (transaction != null) {
            Exception exception = event.getException();
            dispatcher.dispatch(new XAPlusRollbackFailedEvent(xid, exception));
        }
    }

    @Override
    public void handleBranchRolledBack(XAPlusBranchRolledBackEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusXid branchXid = event.getBranchXid();
        state.setRolledBack(xid, branchXid);
        check(xid);
    }

    @Override
    public void handleRollbackBranchFailed(XAPlusRollbackBranchFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusXid branchXid = event.getBranchXid();
        state.setRollbackAsFailed(xid, branchXid);
        check(xid);
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
    public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        state.remove(xid);
    }

    @Override
    public void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        state.remove(xid);
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionFailedEvent.class);
        dispatcher.subscribe(this, XAPlusBranchRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateDoneEvent.class);
        dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (state.check(xid)) {
            XAPlusTransaction transaction = state.getTransaction(xid);
            dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
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

        void setRolledBack(XAPlusXid xid, XAPlusXid branchXid) {
            Set<XAPlusXid> remaining = waiting.get(xid);
            if (remaining != null) {
                remaining.remove(branchXid);
            }
        }

        void setRollbackAsFailed(XAPlusXid xid, XAPlusXid branchXid) {
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