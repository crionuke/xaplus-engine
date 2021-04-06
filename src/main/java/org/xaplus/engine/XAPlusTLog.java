package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
// TODO: remove bqual from tlog as gtrid enough
class XAPlusTLog {
    static final String FIND_DANGLING_TX_SQL = "SELECT t_gtrid, t_bqual, t_unique_name, t_status " +
            "FROM tlog WHERE t_server_id = ? AND t_timestamp < ? " +
            "GROUP BY t_bqual, t_gtrid, t_unique_name, t_status " +
            "HAVING COUNT(*) = 1";
    static final String FIND_TX_STATUS_SQL = "SELECT COUNT(*) AS t_count FROM tlog " +
            "WHERE t_server_id = ? AND t_gtrid = ? GROUP BY t_bqual";
    static final String INSERT_TX_STATUS_SQL = "INSERT INTO tlog " +
            "(t_timestamp, t_server_id, t_gtrid, t_bqual, t_unique_name, t_status, t_complete) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";
    static final String SELECT_SQL = "SELECT t_server_id, t_gtrid, t_bqual, t_unique_name, t_status, t_complete " +
            "FROM tlog";
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTLog.class);

    static private final int FETCH_SIZE = 50;
    private final String serverId;
    private final XAPlusEngine engine;

    XAPlusTLog(String serverId, XAPlusEngine engine) {
        this.serverId = serverId;
        this.engine = engine;
    }

    Map<String, Map<XAPlusXid, Boolean>> findDanglingTransactions(long inflightCutoff) throws SQLException {
        DataSource tlogDataSource = engine.getTLogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            connection.setAutoCommit(false);
            Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(FIND_DANGLING_TX_SQL)) {
                statement.setFetchSize(FETCH_SIZE);
                statement.setString(1, serverId);
                statement.setTimestamp(2, new Timestamp(inflightCutoff));

                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        XAPlusUid gtrid = new XAPlusUid(resultSet.getBytes(1));
                        XAPlusUid bqual = new XAPlusUid(resultSet.getBytes(2));
                        String uniqueName = resultSet.getString(3);
                        boolean tstatus = resultSet.getBoolean(4);
                        if (logger.isTraceEnabled()) {
                            XAPlusXid xid = new XAPlusXid(gtrid, bqual);
                            logger.trace("Dangling transaction found, uniqueName={}, xid={}, status={}",
                                    uniqueName, xid, tstatus);
                        }
                        Map<XAPlusXid, Boolean> xids = danglingTransactions.get(uniqueName);
                        if (xids == null) {
                            xids = new HashMap<>();
                            danglingTransactions.put(uniqueName, xids);
                        }
                        XAPlusXid xid = new XAPlusXid(gtrid, bqual);
                        xids.put(xid, tstatus);
                    }
                }
            }
            if (logger.isDebugEnabled()) {
                StringBuilder debugMessage = new StringBuilder();
                debugMessage.append("Dangling transaction found on " +
                        danglingTransactions.size() + " resources: ");
                for (String uniqueName : danglingTransactions.keySet()) {
                    debugMessage.append(uniqueName + " has " +
                            danglingTransactions.get(uniqueName).size() + "; ");
                }
                logger.debug(debugMessage.toString());
            }
            return danglingTransactions;
        }
    }

    void logCommitXidDecision(XAPlusXid xid, String uniqueName) throws SQLException {
        logXid(xid, uniqueName, true, false);
    }

    void logXidCommitted(XAPlusXid xid, String uniqueName) throws SQLException {
        logXid(xid, uniqueName, true, true);
    }

    void logRollbackXidDecision(XAPlusXid xid, String uniqueName) throws SQLException {
        logXid(xid, uniqueName, false, false);
    }

    void logXidRolledBack(XAPlusXid xid, String uniqueName) throws SQLException {
        logXid(xid, uniqueName, false, true);
    }

    void logCommitTransactionDecision(XAPlusTransaction transaction) throws SQLException {
        log(transaction.getBranches(), true, false);
    }

    void logTransactionCommitted(XAPlusTransaction transaction) throws SQLException {
        log(transaction.getBranches(), true, true);
    }

    void logRollbackTransactionDecision(XAPlusTransaction transaction) throws SQLException {
        log(transaction.getBranches(), false, false);
    }

    void logTransactionRolledBack(XAPlusTransaction transaction) throws SQLException {
        log(transaction.getBranches(), false, true);
    }

    private void logXid(XAPlusXid xid, String uniqueName, boolean tstatus, boolean complete) throws SQLException {
        Map<XAPlusXid, String> uniqueNames = new HashMap<>();
        uniqueNames.put(xid, uniqueName);
        log(uniqueNames, tstatus, complete);
    }

    private void log(Map<XAPlusXid, String> uniqueNames, boolean tstatus, boolean complete) throws SQLException {
        DataSource tlogDataSource = engine.getTLogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(INSERT_TX_STATUS_SQL)) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                statement.setTimestamp(1, timestamp);
                statement.setString(2, serverId);
                statement.setBoolean(6, tstatus);
                statement.setBoolean(7, complete);
                for (Map.Entry<XAPlusXid, String> entry : uniqueNames.entrySet()) {
                    XAPlusXid branchXid = entry.getKey();
                    XAPlusUid branchGtrid = branchXid.getGlobalTransactionIdUid();
                    XAPlusUid branchBqual = branchXid.getBranchQualifierUid();
                    String uniqueName = entry.getValue();
                    statement.setBytes(3, branchGtrid.getArray());
                    statement.setBytes(4, branchBqual.getArray());
                    statement.setString(5, uniqueName);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Log timestamp={}, serverId={}, uniqueName={}, status={}, complete={}, xid={}",
                                timestamp, serverId, uniqueName, tstatus, complete, branchXid);
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }
    }

    static class TransactionStatus {
        final boolean found;
        final boolean completed;

        TransactionStatus(boolean found, boolean completed) {
            this.found = found;
            this.completed = completed;
        }
    }
}
