package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.tm.XAPlusTransactionFinishedEvent;
import org.xaplus.engine.events.journal.*;
import org.xaplus.engine.events.recovery.XAPlusDanglingTransactionCommittedEvent;
import org.xaplus.engine.events.recovery.XAPlusDanglingTransactionRolledBackEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveredXidCommittedEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveredXidRolledBackEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusReportAbsenceXidRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusReportDoneStatusRequestEvent;

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
        XAPlusLogCompletedTransactionEvent.Handler,
        XAPlusLogCommitRecoveredXidDecisionEvent.Handler,
        XAPlusLogRollbackRecoveredXidDecisionEvent.Handler,
        XAPlusRecoveredXidCommittedEvent.Handler,
        XAPlusRecoveredXidRolledBackEvent.Handler,
        XAPlusDanglingTransactionCommittedEvent.Handler,
        XAPlusDanglingTransactionRolledBackEvent.Handler,
        XAPlusFindDanglingTransactionsRequestEvent.Handler,
        XAPlusReportTransactionStatusRequestEvent.Handler,
        XAPlusUserCommitRequestEvent.Handler,
        XAPlusUserRollbackRequestEvent.Handler,
        XAPlusTransactionFinishedEvent.Handler {
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
            tlog.logCommitTransactionDecision(transaction);
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
            tlog.logRollbackTransactionDecision(transaction);
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
    public void handleLogTransactionCompleted(XAPlusLogCompletedTransactionEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        try {
            if (event.isStatus()) {
                tlog.logTransactionCommitted(transaction);
            } else {
                tlog.logTransactionRolledBack(transaction);
            }
            if (logger.isDebugEnabled()) {
                logger.debug("Completed transaction logged, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusCompletedTransactionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log completed transaction failed as {}, xid={}",
                        sqle.getMessage(), xid);
            }
            dispatcher.dispatch(new XAPlusLogComplettedTransactionFailedEvent(transaction, sqle));
        }
    }

    @Override
    public void handleLogCommitRecoveredXidDecision(XAPlusLogCommitRecoveredXidDecisionEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        try {
            tlog.logCommitXidDecision(event.getXid(), event.getUniqueName());
            if (logger.isDebugEnabled()) {
                logger.debug("Commit decision for recovered xid logged, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusCommitRecoveredXidDecisionLoggedEvent(xid, uniqueName));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log commit decision for recovered xid failed as {}, xid={}, xaResource={}",
                        sqle.getMessage(), xid, uniqueName);
            }
        }
    }

    @Override
    public void handleLogRollbackRecoveredXidDecision(XAPlusLogRollbackRecoveredXidDecisionEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        try {
            tlog.logRollbackXidDecision(event.getXid(), event.getUniqueName());
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback decision for recovered xid logged, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusRollbackRecoveredXidDecisionLoggedEvent(xid, uniqueName));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback decision for recovered xid failed as {}, xid={}, xaResource={}",
                        sqle.getMessage(), xid, uniqueName);
            }
        }
    }

    @Override
    public void handleRecoveredXidCommitted(XAPlusRecoveredXidCommittedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        try {
            tlog.logXidCommitted(xid, uniqueName);
            if (logger.isDebugEnabled()) {
                logger.debug("Done status for committed recovered xid logged, xid={}, xaResource={}",
                        xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log done status for committed recovered xid failed as {}, xid={}, xaResource={}",
                        sqle.getMessage(), xid, uniqueName);
            }
        }
    }

    @Override
    public void handleRecoveredXidRolledBack(XAPlusRecoveredXidRolledBackEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        try {
            tlog.logXidRolledBack(xid, uniqueName);
            if (logger.isDebugEnabled()) {
                logger.debug("Done status for rolled back recovered xid logged, xid={}, xaResource={}", xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log done status for rolled back recovered xid failed as {}, xid={}, xaResource={}",
                        sqle.getMessage(), xid, uniqueName);
            }
        }
    }

    @Override
    public void handleDanglingTransactionCommitted(XAPlusDanglingTransactionCommittedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        try {
            tlog.logXidCommitted(xid, uniqueName);
            if (logger.isDebugEnabled()) {
                logger.debug("Commit status for dangling transaction logged, xid={}, xaResource={}",
                        xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log commit status for dangling transaction failed as {}, xid={}, xaResource={}",
                        sqle.getMessage(), xid, uniqueName);
            }
        }
    }

    @Override
    public void handleDanglingTransactionRolledBack(XAPlusDanglingTransactionRolledBackEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        try {
            tlog.logXidRolledBack(xid, uniqueName);
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback status for dangling transaction logged, xid={}, xaResource={}",
                        xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback status for dangling transaction failed as {},  xid={}, xaResource={}",
                        sqle.getMessage(), xid, uniqueName);
            }
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
            Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = tlog.findDanglingTransactions(inflightCutoff);
            dispatcher.dispatch(new XAPlusDanglingTransactionsFoundEvent(danglingTransactions));
        } catch (SQLException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Find dangling transaction from journal failed as {}", e.getMessage());
            }
            dispatcher.dispatch(new XAPlusFindDanglingTransactionsFailedEvent(e));
        }
    }

    @Override
    public void handleReportTransactionStatusRequest(XAPlusReportTransactionStatusRequestEvent event)
            throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        try {
            XAPlusXid xid = event.getXid();
            XAPlusResource resource = event.getResource();

            XAPlusTLog.TransactionStatus status = tlog.getTransactionStatus(xid);
            if (!status.found) {
                dispatcher.dispatch(new XAPlusReportAbsenceXidRequestEvent(xid, resource));
            } else if (status.completed) {
                dispatcher.dispatch(new XAPlusReportDoneStatusRequestEvent(xid, resource));
            } else {
                // TODO: what i can do here?
            }
        } catch (SQLException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Find transaction status from journal failed as {}", e.getMessage());
            }
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
    public void handleTransactionFinished(XAPlusTransactionFinishedEvent event) throws InterruptedException {
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
        dispatcher.subscribe(this, XAPlusLogCompletedTransactionEvent.class);
        dispatcher.subscribe(this, XAPlusLogCommitRecoveredXidDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusLogRollbackRecoveredXidDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveredXidCommittedEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveredXidRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusDanglingTransactionCommittedEvent.class);
        dispatcher.subscribe(this, XAPlusDanglingTransactionRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusFindDanglingTransactionsRequestEvent.class);
        dispatcher.subscribe(this, XAPlusReportTransactionStatusRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserCommitRequestEvent.class);
        dispatcher.subscribe(this, XAPlusUserRollbackRequestEvent.class);
        dispatcher.subscribe(this, XAPlusTransactionFinishedEvent.class);
    }
}
