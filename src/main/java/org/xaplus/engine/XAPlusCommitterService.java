package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusCommitTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionFailedEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlusCommitTransactionEvent;
import org.xaplus.engine.events.twopc.XAPlusTransactionCommittedEvent;
import org.xaplus.engine.events.twopc.XAPlusTransactionPreparedEvent;
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
        XAPlusTransactionTimedOutEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusCommitterService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusResources resources;
    private final XAPlusTracker tracker;

    XAPlusCommitterService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                           XAPlusResources resources, XAPlusTracker tracker) {
        super(properties.getServerId() + "-committer", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.resources = resources;
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
                if (logger.isDebugEnabled()) {
                    logger.debug("Log commit decision, {}", transaction);
                }
                dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
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
            if (logger.isDebugEnabled()) {
                logger.debug("Log commit decision, {}", transaction);
            }
            dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
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
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Commit transaction, {}", transaction);
            }
            transaction.commit(dispatcher);
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
            if (logger.isDebugEnabled()) {
                logger.debug("Log commit decision failed, {}", transaction);
            }
            dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction, true));
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
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Branch committed, xid={}, {}", branchXid, transaction);
            }
            transaction.branchCommitted(branchXid, false);
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
            XAPlusXid branchXid = event.getBranchXid();
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            if (logger.isDebugEnabled()) {
                logger.debug("Commit branch failed, xid={}, {}", branchXid, transaction);
            }
            transaction.branchCommitted(branchXid, true);
            check(xid);
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
            XAPlusTransaction transaction = tracker.getTransaction(transactionXid);
            transaction.branchDone(branchXid);
            if (logger.isDebugEnabled()) {
                String subordinateServerId = branchXid.getGlobalTransactionIdUid().extractServerId();
                logger.debug("Branch done, subordinateServerId={}, xid={}, {}",
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
    public void handleTransactionTimedOut(XAPlusTransactionTimedOutEvent event) {
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
        dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
    }

    private void check(XAPlusXid xid) throws InterruptedException {
        if (tracker.contains(xid)) {
            XAPlusTransaction transaction = tracker.getTransaction(xid);
            if (transaction.isCommitted()) {
                if (transaction.hasFailures()) {
                    dispatcher.dispatch(new XAPlus2pcFailedEvent(tracker.remove(xid), false));
                } else if (transaction.isDone()) {
                    dispatcher.dispatch(new XAPlusTransactionCommittedEvent(tracker.remove(xid)));
                }
            }
        }
    }
}
