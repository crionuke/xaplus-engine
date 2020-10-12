package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.xaplus.engine.events.*;
import org.xaplus.engine.events.journal.*;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;

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
        XAPlusDanglingTransactionCommittedEvent.Handler,
        XAPlusDanglingTransactionRolledBackEvent.Handler,
        XAPlus2pcDoneEvent.Handler,
        XAPlusFindDanglingTransactionsRequestEvent.Handler {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusJournalService.class);

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
            tlog.logCommitTransactionDecision(transaction);
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
                logger.debug("Rollback decision for transaction with xid={} logged", xid);
            }
            dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback decision for transaction with xid={} failed with {}", sqle.getMessage());
            }
            dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionFailedEvent(transaction, sqle));
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
            tlog.logRollbackXidDecision(event.getXid(), event.getUniqueName());
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
            tlog.logXidCommitted(xid, uniqueName);
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
            tlog.logXidRolledBack(xid, uniqueName);
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
    public void handleDanglingTransactionCommitted(XAPlusDanglingTransactionCommittedEvent event) throws InterruptedException {
        if (logger.isTraceEnabled()) {
            logger.trace("Handle {}", event);
        }
        XAPlusXid xid = event.getXid();
        String uniqueName = event.getUniqueName();
        try {
            tlog.logXidCommitted(xid, uniqueName);
            if (logger.isDebugEnabled()) {
                logger.debug("Commit status for dangling transaction with xid={} on resource={} logged",
                        xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log commit status for dangling transaction with xid={} on resource={} failed with {}",
                        xid, uniqueName, sqle.getMessage());
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
                logger.debug("Rollback status for dangling transaction with xid={} on resource={} logged",
                        xid, uniqueName);
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback status for dangling transaction with xid={} on resource={} failed with {}",
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
            tlog.logTransactionCommitted(transaction);
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
                logger.warn("Find dangling transaction from journal failed with {}", e.getMessage());
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
        dispatcher.subscribe(this, XAPlusDanglingTransactionCommittedEvent.class);
        dispatcher.subscribe(this, XAPlusDanglingTransactionRolledBackEvent.class);
        dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        dispatcher.subscribe(this, XAPlusFindDanglingTransactionsRequestEvent.class);
    }
}
