package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.*;
import org.xaplus.engine.events.recovery.XAPlusDanglingTransactionCommittedEvent;
import org.xaplus.engine.events.recovery.XAPlusDanglingTransactionRolledBackEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveredXidCommittedEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveredXidRolledBackEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionClosedEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;

import java.sql.SQLException;
import java.util.Comparator;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusJournalService extends Bolt implements
        XAPlusLogCommitTransactionDecisionEvent.Handler,
        XAPlusLogRollbackTransactionDecisionEvent.Handler,
        XAPlusFindDanglingTransactionsRequestEvent.Handler,
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlusTransactionClosedEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusJournalService.class);

    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusTLog tlog;
    private final long startTime;
    private final SortedSet<XAPlusTransaction> inFlightTransactions;

    XAPlusJournalService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                         XAPlusTLog tlog) {
        super(properties.getServerId() + "-journal", properties.getQueueSize());
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.tlog = tlog;
        startTime = System.currentTimeMillis();
        inFlightTransactions = new TreeSet<>(Comparator
                .comparingLong((transaction) -> transaction.getCreationTimeInMillis()));
    }

    @Override
    public void handleLogCommitTransactionDecision(XAPlusLogCommitTransactionDecisionEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        try {
            tlog.logCommitDecision(xid.getGlobalTransactionIdUid());
            if (logger.isDebugEnabled()) {
                logger.debug("Commit decision logged, xid={}, branches={}", xid, transaction.getBranches());
            }
            dispatcher.dispatch(new XAPlusCommitTransactionDecisionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log commit decision failed as {}, xid={}, branches={}",
                        sqle.getMessage(), xid, transaction.getBranches());
            }
            dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionFailedEvent(transaction, sqle));
        }
    }

    @Override
    public void handleLogRollbackTransactionDecision(XAPlusLogRollbackTransactionDecisionEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        try {
            tlog.logRollbackDecision(xid.getGlobalTransactionIdUid());
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback decision logged, xid={}, branches={}", xid, transaction.getBranches());
            }
            dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback decision failed as {}, xid={}", sqle.getMessage(), xid);
            }
            dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionFailedEvent(transaction, sqle));
        }
    }

    @Override
    public void handleFindDanglingTransactionsRequest(XAPlusFindDanglingTransactionsRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        try {
            long inflightCutoff;
            if (inFlightTransactions.isEmpty()) {
                // If no transaction yet use app start time
                inflightCutoff = startTime;
            } else {
                inflightCutoff = inFlightTransactions.first().getCreationTimeInMillis();
            }
            Map<XAPlusUid, Boolean> danglingTransactions = tlog.findDanglingTransactions(inflightCutoff);
            dispatcher.dispatch(new XAPlusDanglingTransactionsFoundEvent(danglingTransactions));
        } catch (SQLException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Find dangling transaction from journal failed as {}", e.getMessage());
            }
            dispatcher.dispatch(new XAPlusFindDanglingTransactionsFailedEvent(e));
        }
    }

    @Override
    public void handleUserCommitRequest(XAPlusUserCommitRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        inFlightTransactions.add(transaction);
        if (logger.isTraceEnabled()) {
            logger.trace("Transaction added to in-flight list, {}", transaction);
        }
    }

    @Override
    public void handleUserRollbackRequest(XAPlusUserRollbackRequestEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        inFlightTransactions.add(transaction);
        if (logger.isTraceEnabled()) {
            logger.trace("Transaction added to in-flight list, {}", transaction);
        }
    }

    @Override
    public void handleTransactionClosed(XAPlusTransactionClosedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        if (inFlightTransactions.remove(transaction)) {
            if (logger.isDebugEnabled()) {
                logger.debug("Transaction removed from in-flight list, {}", transaction);
            }
        }
    }

    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusFindDanglingTransactionsRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionClosedEvent.class);
    }
}
