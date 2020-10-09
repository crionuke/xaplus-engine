package org.xaplus;

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

    int FETCH_SIZE = 50;

    // Rollback, commit and done statuses
    enum TSTATUS {
        R, C, D
    }

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
            String sql = "SELECT t_gtrid, t_bqual, t_unique_name, SUM(t_status) FROM tlog " +
                    "WHERE t_server_id=? GROUP BY t_bqual, t_gtrid, t_unique_name HAVING SUM(t_status) < 2;";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setFetchSize(FETCH_SIZE);
                statement.setString(1, properties.getServerId());
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        XAPlusUid gtrid = new XAPlusUid(resultSet.getBytes(1));
                        XAPlusUid bqual = new XAPlusUid(resultSet.getBytes(2));
                        String uniqueName = resultSet.getString(3);
                        TSTATUS tstatus = TSTATUS.values()[resultSet.getInt(4)];
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
            return danglingTransactions;
        }
    }

    void log(XAPlusXid xid, String uniqueName, TSTATUS tstatus) throws SQLException {
        Map<XAPlusXid, String> uniqueNames = new HashMap<>();
        uniqueNames.put(xid, uniqueName);
        log(uniqueNames, tstatus);
    }

    void log(XAPlusTransaction transaction, TSTATUS tstatus) throws SQLException {
        log(transaction.getUniqueNames(), tstatus);
    }

    private void log(Map<XAPlusXid, String> uniqueNames, TSTATUS tstatus) throws SQLException {
        DataSource tlogDataSource = engine.getTlogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            String sql = "INSERT INTO tlog (t_timestamp, t_server_id, t_gtrid, t_bqual, t_unique_name, t_status) "
                    + "VALUES(?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                statement.setString(2, properties.getServerId());
                statement.setInt(6, tstatus.ordinal());
                for (Map.Entry<XAPlusXid, String> entry : uniqueNames.entrySet()) {
                    XAPlusXid branchXid = entry.getKey();
                    XAPlusUid branchGtrid = branchXid.getGlobalTransactionIdUid();
                    XAPlusUid branchBqual = branchXid.getBranchQualifierUid();
                    String uniqueName = entry.getValue();
                    statement.setBytes(3, branchGtrid.getArray());
                    statement.setBytes(4, branchBqual.getArray());
                    statement.setString(5, uniqueName);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Log with serverId={}, uniqueName={}, xid={}, status={}",
                                properties.getServerId(), uniqueName, branchXid, tstatus);
                    }
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }
    }
}
