package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.*;
import com.crionuke.xaplus.events.twopc.XAPlus2pcDoneEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
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
        Map<XAPlusXid, String> uniqueNames = transaction.getUniqueNames();
        try {
            tlog.log(uniqueNames, XAPlusTLog.TSTATUS.C);
            if (logger.isDebugEnabled()) {
                logger.debug("Commit decision for transaction with xid={} and resources={} logged", xid, uniqueNames);
            }
            dispatcher.dispatch(new XAPlusCommitTransactionDecisionLoggedEvent(transaction));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log commit decision for transaction with xid={} and resources={} failed with {}",
                        xid, sqle.getMessage(), uniqueNames);
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
        XAPlusXid xid = event.getXid();
        Map<XAPlusXid, String> uniqueNames = event.getUniqueNames();
        try {
            tlog.log(uniqueNames, XAPlusTLog.TSTATUS.R);
            if (logger.isDebugEnabled()) {
                logger.debug("Rollback decision for transaction with xid={} logged", xid);
            }
            dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(xid));
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Log rollback decision for transaction with xid={} failed with {}", sqle.getMessage());
            }
            dispatcher.dispatch(new XAPlusRollbackTransactionDecisionFailedEvent(xid, sqle));
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
        Map<XAPlusXid, String> uniqueNames = transaction.getUniqueNames();
        try {
            tlog.log(uniqueNames, XAPlusTLog.TSTATUS.D);
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
            DataSource tlogDataSource = engine.getTlogDataSource();
            try (Connection connection = tlogDataSource.getConnection()) {
                connection.setAutoCommit(false);
                Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = new HashMap<>();
                String sql = "SELECT t_gtrid, t_bqual, t_unique_name, t_status FROM tlog " +
                        "WHERE t_server_id=? GROUP BY t_bqual, t_gtrid, t_unique_name, t_status HAVING COUNT(*) = 1;";
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    statement.setFetchSize(FETCH_SIZE);
                    statement.setString(1, properties.getServerId());
                    try (ResultSet resultSet = statement.executeQuery()) {
                        while (resultSet.next()) {
                            XAPlusUid gtrid = new XAPlusUid(resultSet.getBytes(1));
                            XAPlusUid bqual = new XAPlusUid(resultSet.getBytes(2));
                            String uniqueName = resultSet.getString(3);
                            XAPlusTLog.TSTATUS tstatus = XAPlusTLog.TSTATUS.valueOf(resultSet.getString(4));
                            if (logger.isDebugEnabled()) {
                                logger.debug("JOURNAL: retry uniqueName={}, bqual={} and gtrid={}, " +
                                        "status={} from tlog", uniqueName, bqual, gtrid, tstatus.name());
                            }
                            Map<XAPlusXid, Boolean> xids = danglingTransactions.get(uniqueName);
                            if (xids == null) {
                                xids = new HashMap<>();
                                danglingTransactions.put(uniqueName, xids);
                            }
                            XAPlusXid xid = new XAPlusXid(gtrid, bqual);
                            if (tstatus == XAPlusTLog.TSTATUS.C) {
                                xids.put(xid, true);
                            } else if (tstatus == XAPlusTLog.TSTATUS.R) {
                                xids.put(xid, false);
                            } else {
                                // TODO: wrong data in db ??
                            }
                        }
                    }
                }
                if (logger.isDebugEnabled()) {
                    StringBuilder debugMessage = new StringBuilder();
                    debugMessage.append("Dangling transaction found on " +
                            danglingTransactions.size() + " resources:\n");
                    for (String uniqueName : danglingTransactions.keySet()) {
                        debugMessage.append("Resource " + uniqueName + " has " +
                                danglingTransactions.get(uniqueName).size() + " dangling transactions");
                    }
                    logger.debug(debugMessage.toString());
                }
                dispatcher.dispatch(new XAPlusDanglingTransactionsFoundEvent(danglingTransactions));
            }
        } catch (SQLException sqle) {
            if (logger.isWarnEnabled()) {
                logger.warn("Recovery dangling transaction from journal failed with {}", sqle.getMessage());
            }
            dispatcher.dispatch(new XAPlusFindDanglingTransactionsFailedEvent(sqle));
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
