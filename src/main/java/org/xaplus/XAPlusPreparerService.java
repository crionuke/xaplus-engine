package org.xaplus;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.events.*;
import org.xaplus.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.events.xaplus.XAPlusRemoteSubordinateReadyEvent;
import org.xaplus.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;

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
class XAPlusPreparerService extends Bolt implements
        XAPlus2pcRequestEvent.Handler,
        XAPlusPrepareTransactionEvent.Handler,
        XAPlusBranchPreparedEvent.Handler,
        XAPlusBranchReadOnlyEvent.Handler,
        XAPlusPrepareBranchFailedEvent.Handler,
        XAPlusRemoteSubordinateReadyEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusTimeoutEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPreparerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final State state;

    XAPlusPreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher) {
        super("preparer", properties.getQueueSize());
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
        if (transaction.isSuperior()) {
            prepare(transaction);
        }
    }

    @Override
    public void handlePrepareTransaction(XAPlusPrepareTransactionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        prepare(transaction);
    }

    @Override
    public void handleBranchPrepared(XAPlusBranchPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusXid branchXid = event.getBranchXid();
        state.setPrepared(xid, branchXid);
        check(xid);
    }

    @Override
    public void handleBranchReadOnly(XAPlusBranchReadOnlyEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        XAPlusXid branchXid = event.getBranchXid();
        state.setPrepared(xid, branchXid);
        check(xid);
    }

    @Override
    public void handlePrepareBranchFailed(XAPlusPrepareBranchFailedEvent event) throws InterruptedException {
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
    public void handleRemoteSubordinateReady(XAPlusRemoteSubordinateReadyEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getXid();
        state.setReady(branchXid);
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
    public void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException {
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

    private void prepare(XAPlusTransaction transaction) throws InterruptedException {
        if (state.track(transaction)) {
            XAPlusXid xid = transaction.getXid();
            Map<XAPlusXid, XAResource> resources = new HashMap<>();
            resources.putAll(transaction.getXaResources());
            resources.putAll(transaction.getXaPlusResources());
            for (Map.Entry<XAPlusXid, XAResource> entry : resources.entrySet()) {
                XAPlusXid branchXid = entry.getKey();
                XAResource resource = entry.getValue();
                dispatcher.dispatch(new XAPlusPrepareBranchRequestEvent(xid, branchXid, resource));
            }
        }
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (state.check(xid)) {
            XAPlusTransaction transaction = state.remove(xid);
            dispatcher.dispatch(new XAPlusTransactionPreparedEvent(transaction));
        }
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
        dispatcher.subscribe(this, XAPlusPrepareTransactionEvent.class);
        dispatcher.subscribe(this, XAPlusBranchPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusBranchReadOnlyEvent.class);
        dispatcher.subscribe(this, XAPlusPrepareBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateReadyEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }

    private class State {
        private final Map<XAPlusXid, XAPlusTransaction> transactions;
        private final Map<XAPlusXid, XAPlusXid> branchToTransactionXids;
        private final Map<XAPlusXid, Set<XAPlusXid>> preparing;
        private final Map<XAPlusXid, Set<XAPlusXid>> waiting;

        State() {
            transactions = new HashMap<>();
            branchToTransactionXids = new HashMap<>();
            preparing = new HashMap<>();
            waiting = new HashMap<>();
        }

        boolean track(XAPlusTransaction transaction) {
            XAPlusXid xid = transaction.getXid();
            if (transactions.put(xid, transaction) == null) {
                HashSet preparingBranches = new HashSet();
                HashSet waitingBranches = new HashSet();
                transaction.getXaResources().forEach((x, r) -> {
                    preparingBranches.add(x);
                });
                transaction.getXaPlusResources().forEach((x, r) -> {
                    waitingBranches.add(x);
                    branchToTransactionXids.put(x, xid);
                });
                preparing.put(xid, preparingBranches);
                waiting.put(xid, waitingBranches);
                return true;
            } else {
                return false;
            }
        }

        XAPlusTransaction getTransaction(XAPlusXid xid) {
            return transactions.get(xid);
        }

        XAPlusXid getTransactionXid(XAPlusXid branchXid) {
            return branchToTransactionXids.get(branchXid);
        }

        void setPrepared(XAPlusXid xid, XAPlusXid branchXid) {
            Set<XAPlusXid> remaining = preparing.get(xid);
            if (remaining != null) {
                remaining.remove(branchXid);
            }
        }

        void setReady(XAPlusXid branchXid) {
            XAPlusXid xid = branchToTransactionXids.get(branchXid);
            if (xid != null) {
                Set<XAPlusXid> remaining = waiting.get(xid);
                if (remaining != null) {
                    remaining.remove(branchXid);
                }
            }
        }

        XAPlusTransaction remove(XAPlusXid xid) {
            XAPlusTransaction transaction = transactions.remove(xid);
            if (transaction != null) {
                transaction.getXaPlusResources().forEach((x, r) -> branchToTransactionXids.remove(x));
                preparing.remove(xid);
                waiting.remove(xid);
                return transaction;
            } else {
                return null;
            }
        }

        Boolean check(XAPlusXid xid) {
            if (transactions.containsKey(xid) && preparing.containsKey(xid) && waiting.containsKey(xid)) {
                return preparing.get(xid).isEmpty() && waiting.get(xid).isEmpty();
            } else {
                return false;
            }
        }
    }
}