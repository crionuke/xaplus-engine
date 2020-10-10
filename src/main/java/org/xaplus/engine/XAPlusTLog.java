package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;

@Component
class XAPlusTLog {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTLog.class);

    static final String CREATE_SQL = "CREATE TABLE tlog (" +
            "t_id bigserial PRIMARY KEY, " +
            "t_timestamp timestamp NOT NULL, " +
            "t_server_id varchar(64) NOT NULL, " +
            "t_unique_name varchar(64) NOT NULL, " +
            "t_gtrid bytea, " +
            "t_bqual bytea, " +
            "t_status boolean NOT NULL" +
            "t_complete boolean NOT NULL" +
            ");";

    static private final String SELECT_SQL = "SELECT t_gtrid, t_bqual, t_unique_name, t_status " +
            "FROM tlog WHERE t_server_id=? " +
            "GROUP BY t_bqual, t_gtrid, t_unique_name, t_status " +
            "HAVING COUNT(*) = 1;";

    static private final String INSERT_SQL = "INSERT INTO tlog " +
            "(t_timestamp, t_server_id, t_gtrid, t_bqual, t_unique_name, t_status, t_complete) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?)";

    int FETCH_SIZE = 50;

    private final XAPlusProperties properties;
    private final XAPlusEngine engine;

    XAPlusTLog(XAPlusProperties properties, XAPlusEngine engine) {
        this.properties = properties;
        this.engine = engine;
    }

    Map<String, Map<XAPlusXid, Boolean>> findDanglingTransactions() throws SQLException {
        DataSource tlogDataSource = engine.getTlogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            connection.setAutoCommit(false);
            Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(SELECT_SQL)) {
                statement.setFetchSize(FETCH_SIZE);
                statement.setString(1, properties.getServerId());
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        XAPlusUid gtrid = new XAPlusUid(resultSet.getBytes(1));
                        XAPlusUid bqual = new XAPlusUid(resultSet.getBytes(2));
                        String uniqueName = resultSet.getString(3);
                        boolean tstatus = resultSet.getBoolean(4);
                        if (logger.isDebugEnabled()) {
                            XAPlusXid xid = new XAPlusXid(gtrid, bqual);
                            logger.debug("Dangling transaction found with uniqueName={}, xid={}, status={}",
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
                        danglingTransactions.size() + " resources:\n");
                for (String uniqueName : danglingTransactions.keySet()) {
                    debugMessage.append("Resource " + uniqueName + " has " +
                            danglingTransactions.get(uniqueName).size() + " dangling transactions");
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
        log(transaction.getUniqueNames(), true, false);
    }

    void logTransactionCommitted(XAPlusTransaction transaction) throws SQLException {
        log(transaction.getUniqueNames(), true, true);
    }

    void logRollbackTransactionDecision(XAPlusTransaction transaction) throws SQLException {
        log(transaction.getUniqueNames(), false, false);
    }

    void logTransactionRolledBack(XAPlusTransaction transaction) throws SQLException {
        log(transaction.getUniqueNames(), false, true);
    }

    private void logXid(XAPlusXid xid, String uniqueName, boolean tstatus, boolean complete) throws SQLException {
        Map<XAPlusXid, String> uniqueNames = new HashMap<>();
        uniqueNames.put(xid, uniqueName);
        log(uniqueNames, tstatus, complete);
    }

    private void log(Map<XAPlusXid, String> uniqueNames, boolean tstatus, boolean complete) throws SQLException {
        DataSource tlogDataSource = engine.getTlogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
                statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                statement.setString(2, properties.getServerId());
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
                        logger.debug("Log serverId={}, uniqueName={}, xid={}, status={}, complete={}",
                                properties.getServerId(), uniqueName, branchXid, tstatus, complete);
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }
    }
}
