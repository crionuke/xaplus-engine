package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.rollback.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.twopc.XAPlusPrepareTransactionEvent;
import org.xaplus.engine.events.twopc.XAPlusTransactionPreparedEvent;
import org.xaplus.engine.events.xa.XAPlusBranchPreparedEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateReadyEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusPreparerService extends Bolt implements
        XAPlus2pcRequestEvent.Handler,
        XAPlusPrepareTransactionEvent.Handler,
        XAPlusBranchPreparedEvent.Handler,
        XAPlusPrepareBranchFailedEvent.Handler,
        XAPlusRemoteSubordinateReadyEvent.Handler,
        XAPlus2pcFailedEvent.Handler,
        XAPlusTransactionTimedOutEvent.Handler,
        XAPlusRemoteSuperiorOrderToRollbackEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPreparerService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTracker tracker;

    XAPlusPreparerService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                          XAPlusTracker tracker) {
        super(properties.getServerId() + "-preparer", properties.getQueueSize());
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
        if (transaction.isSuperior() && tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Prepare transaction, {}", transaction);
            }
            transaction.prepare(dispatcher);
        }
    }

    @Override
    public void handlePrepareTransaction(XAPlusPrepareTransactionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Prepare transaction, {}", transaction);
            }
            transaction.prepare(dispatcher);
        }
    }

    @Override
    public void handleBranchPrepared(XAPlusBranchPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusXid branchXid = event.getBranchXid();
            tracker.getTransaction(xid).branchPrepared(branchXid);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch prepared, xid={}, {}", branchXid, tracker.getTransaction(xid));
            }
            check(xid);
        }
    }

    @Override
    public void handlePrepareBranchFailed(XAPlusPrepareBranchFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            Exception exception = event.getException();
            if (logger.isDebugEnabled()) {
                logger.debug("Prepare branch failed, xid={}, {}", event.getBranchXid(), transaction);
            }
            dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction, exception));
        }
    }

    @Override
    public void handleRemoteSubordinateReady(XAPlusRemoteSubordinateReadyEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid branchXid = event.getXid();
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            transaction.branchReadied(branchXid);
            if (logger.isDebugEnabled()) {
                String subordinateServerId = branchXid.getGlobalTransactionIdUid().extractServerId();
                logger.debug("Branch ready, subordinateServerId={}, xid={}, {}",
                        subordinateServerId, branchXid, transaction);
            }
            check(transactionXid);
        }
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
    public void handleTransactionTimedOut(XAPlusTransactionTimedOutEvent event) throws InterruptedException {
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
        XAPlusTransaction transaction = tracker.remove(xid);
        if (transaction != null) {
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
        dispatcher.subscribe(this, XAPlusPrepareTransactionEvent.class);
        dispatcher.subscribe(this, XAPlusBranchPreparedEvent.class);
        dispatcher.subscribe(this, XAPlusPrepareBranchFailedEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSubordinateReadyEvent.class);
        dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
        dispatcher.subscribe(this, XAPlusRemoteSuperiorOrderToRollbackEvent.class);
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            if (transaction.isPrepared() && transaction.isReadied()) {
                tracker.remove(xid);
                dispatcher.dispatch(new XAPlusTransactionPreparedEvent(transaction));
            }
        }
    }
}