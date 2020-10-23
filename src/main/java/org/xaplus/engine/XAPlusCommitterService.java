package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateDoneEvent;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
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
    private final XAPlusTracker tracker;


    XAPlusCommitterService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                           XAPlusTracker tracker) {
        super(properties.getServerId() + "-committer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.tracker = tracker;
    }

    @Override
    public void handleTransactionPrepared(XAPlusTransactionPreparedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (transaction.isSuperior()) {
            if (tracker.track(transaction)) {
                dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
            }
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction is not superior, {}", transaction);
            }
        }
    }

    @Override
    public void handleCommitTransaction(XAPlusCommitTransactionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (tracker.track(transaction)) {
            dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
        } else {
            if (logger.isTraceEnabled()) {
                logger.trace("Transaction already tracked, {}", transaction);
            }
        }
    }

    @Override
    public void handleCommitTransactionDecisionLogged(XAPlusCommitTransactionDecisionLoggedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            tracker.getTransaction(xid).commit(dispatcher);
        }
    }

    @Override
    public void handleLogCommitTransactionDecisionFailed(XAPlusLogCommitTransactionDecisionFailedEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getTransaction().getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
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
        if (tracker.contains(xid)) {
            XAPlusXid branchXid = event.getBranchXid();
            tracker.getTransaction(xid).branchCommitted(branchXid);
            check(xid);
        }
    }

    @Override
    public void handleCommitBranchFailed(XAPlusCommitBranchFailedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
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
        XAPlusXid transactionXid = tracker.getTransactionXid(branchXid);
        if (transactionXid != null && tracker.contains(transactionXid)) {
            tracker.getTransaction(transactionXid).branchDone(branchXid);
            check(transactionXid);
        }
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

    private void check(XAPlusXid xid) throws InterruptedException {
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            if (transaction.isCommitted() && transaction.isDone()) {
                tracker.remove(xid);
                dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
            }
        }
    }
}
