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
class XAPlusTLog {
    static final String DANGLING_SQL = "SELECT t_gtrid, t_status FROM tlog WHERE t_server_id = ? AND t_timestamp < ? GROUP BY t_gtrid, t_status HAVING COUNT(*) = 1";
    static final String INSERT_SQL = "INSERT INTO tlog (t_timestamp, t_server_id, t_gtrid, t_status) VALUES (?, ?, ?, ?)";

    static private final Logger logger = LoggerFactory.getLogger(XAPlusTLog.class);
    static private final int FETCH_SIZE = 50;

    private final String serverId;
    private final XAPlusEngine engine;

    XAPlusTLog(String serverId, XAPlusEngine engine) {
        this.serverId = serverId;
        this.engine = engine;
    }

    Map<XAPlusUid, Boolean> findDanglingTransactions(long inflightCutoff) throws SQLException {
        DataSource tlogDataSource = engine.getTLogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            Map<XAPlusUid, Boolean> danglingTransactions = new HashMap<>();
            try (PreparedStatement statement = connection.prepareStatement(DANGLING_SQL)) {
                statement.setFetchSize(FETCH_SIZE);
                statement.setString(1, serverId);
                statement.setTimestamp(2, new Timestamp(inflightCutoff));
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        XAPlusUid gtrid = new XAPlusUid(resultSet.getBytes(1));
                        boolean tstatus = resultSet.getBoolean(2);
                        if (logger.isTraceEnabled()) {
                            logger.trace("Dangling transaction found, gtrid={}, status={}", gtrid, tstatus);
                        }
                        danglingTransactions.put(gtrid, tstatus);
                    }
                }
            }
            if (logger.isDebugEnabled()) {
                logger.debug("{} dangling transactions found.", danglingTransactions.size());
            }
            return danglingTransactions;
        }
    }

    void logCommitDecision(XAPlusUid gtrid) throws SQLException {
        log(gtrid,true);
    }

    void logRollbackDecision(XAPlusUid gtrid) throws SQLException {
        log(gtrid, false);
    }

    private void log(XAPlusUid gtrid, boolean tstatus) throws SQLException {
        DataSource tlogDataSource = engine.getTLogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(INSERT_SQL)) {
                Timestamp timestamp = new Timestamp(System.currentTimeMillis());
                statement.setTimestamp(1, timestamp);
                statement.setString(2, serverId);
                statement.setBytes(3, gtrid.getArray());
                statement.setBoolean(4, tstatus);
                if (logger.isDebugEnabled()) {
                    logger.debug("Log timestamp={}, serverId={}, status={}, gtrid={}",
                            timestamp, serverId, tstatus, gtrid);
                }
                statement.executeUpdate();
            }
        }
    }
}
