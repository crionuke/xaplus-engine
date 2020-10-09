package org.xaplus;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.events.*;
import org.xaplus.events.twopc.XAPlus2pcDoneEvent;

import javax.annotation.PostConstruct;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Service
class XAPlusJournalService extends Bolt implements
        XAPlusLogCommitTransactionDecisionEvent.Handler,
        XAPlusLogRollbackTransactionDecisionEvent.Handler,
        XAPlusLogCommitRecoveredXidDecisionEvent.Handler,
        XAPlusLogRollbackRecoveredXidDecisionEvent.Handler,
        XAPlusRecoveredXidCommittedEvent.Handler,
        XAPlusRecoveredXidRolledBackEvent.Handler,
        XAPlusDanglingTransactionDoneEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlusFindDanglingTransactionsRequestEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusJournalService.class);

    static private final int FETCH_SIZE = 50;
    private final XAPlusProperties properties;
    private final XAPlusThreadPool threadPool;
    private final XAPlusDispatcher dispatcher;
    private final XAPlusEngine engine;
    private final XAPlusTLog tlog;

    XAPlusJournalService(XAPlusProperties properties, XAPlusThreadPool threadPool, XAPlusDispatcher dispatcher,
                         XAPlusEngine engine, XAPlusTLog tlog) {
        super("journal", properties.getQueueSize());
        this.properties = properties;
        this.threadPool = threadPool;
        this.dispatcher = dispatcher;
        this.engine = engine;
        this.tlog = tlog;
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
            tlog.log(transaction, XAPlusTLog.TSTATUS.C);
            if (logger.isDebugEnabled()) {
                logger.debug("Commit decision for transaction with xid={} and resources={} logged",
                        xid, transaction.getUniqueNames());
            }
            dispatcher.dispatch(new XAPlusCommitTransactionDecisionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log commit decision for transaction with xid={} and resources={} failed with {}",
                        xid, sqle.getMessage(), transaction.getUniqueNames());
            }
            dispatcher.dispatch(new XAPlusCommitTransactionDecisionFailedEvent(transaction, sqle));
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
            tlog.log(transaction, XAPlusTLog.TSTATUS.R);
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback decision for transaction with xid={} logged", xid);
            }
            dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback decision for transaction with xid={} failed with {}", sqle.getMessage());
            }
            dispatcher.dispatch(new XAPlusRollbackTransactionDecisionFailedEvent(transaction, sqle));
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
            tlog.log(event.getXid(), event.getUniqueName(), XAPlusTLog.TSTATUS.C);
            if (logger.isDebugEnabled()) {
                logger.debug("Commit decision for recovered xid={} logged", xid);
            }
            dispatcher.dispatch(new XAPlusCommitRecoveredXidDecisionLoggedEvent(xid, uniqueName));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log commit decision for recovered xid={} on resource={} failed with {}",
                        xid, uniqueName, sqle.getMessage());
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
            tlog.log(event.getXid(), event.getUniqueName(), XAPlusTLog.TSTATUS.R);
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback decision for recovered xid={} logged", xid);
            }
            dispatcher.dispatch(new XAPlusRollbackRecoveredXidDecisionLoggedEvent(xid, uniqueName));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback decision for recovered xid={} on resource={} failed with {}",
                        xid, uniqueName, sqle.getMessage());
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
            tlog.log(xid, uniqueName, XAPlusTLog.TSTATUS.D);
            if (logger.isDebugEnabled()) {
                logger.debug("Done status for committed recovered xid={} on resource={} logged", xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log done status for committed recovered xid={} on resource={} failed with {}",
                        xid, uniqueName, sqle.getMessage());
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
            tlog.log(xid, uniqueName, XAPlusTLog.TSTATUS.D);
            if (logger.isDebugEnabled()) {
                logger.debug("Done status for rolled back recovered xid={} on resource={} logged", xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log done status for rolled back recovered xid={} on resource={} failed with {}",
                        xid, uniqueName, sqle.getMessage());
            }
        }
    }

    @Override
    public void handleDanglingTransactionDone(XAPlusDanglingTransactionDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        try {
            tlog.log(xid, uniqueName, XAPlusTLog.TSTATUS.D);
            if (logger.isDebugEnabled()) {
                logger.debug("Done status for dangling transaction with xid={} on resource={} logged",
                        xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log done status for dangling transaction with xid={} on resource={} failed with {}",
                        xid, uniqueName, sqle.getMessage());
            }
        }
    }

    @Override
    public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusTransaction transaction = event.getTransaction();
        XAPlusXid xid = transaction.getXid();
        try {
            tlog.log(transaction, XAPlusTLog.TSTATUS.D);
            if (logger.isDebugEnabled()) {
                logger.debug("Done status for 2pc transaction with xid={} logged", xid);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log done status for 2pc transaction with xid={} failed with {}",
                        xid, sqle.getMessage());
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
            Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = tlog.findDanglingTransactions();
            dispatcher.dispatch(new XAPlusDanglingTransactionsFoundEvent(danglingTransactions));
        } catch (SQLException e) {
            if (logger.isWarnEnabled()) {
                logger.warn("Recovery dangling transaction from journal failed with {}", e.getMessage());
            }
            dispatcher.dispatch(new XAPlusFindDanglingTransactionsFailedEvent(e));
        }
    }

    @PostConstruct
    void postConstruct() {
        threadPool.execute(this);
        dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusLogCommitRecoveredXidDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusLogRollbackRecoveredXidDecisionEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveredXidCommittedEvent.class);
        dispatcher.subscribe(this, XAPlusRecoveredXidRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlusDanglingTransactionDoneEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlusFindDanglingTransactionsRequestEvent.class);
    }
}
