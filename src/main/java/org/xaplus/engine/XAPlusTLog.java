package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusTLog {
    static final String SELECT_SQL = "SELECT t_status FROM tlog WHERE t_gtrid = ?";
    static final String INSERT_SQL = "INSERT INTO tlog (t_timestamp, t_server_id, t_gtrid, t_status) VALUES (?, ?, ?, ?)";

    static private final Logger logger = LoggerFactory.getLogger(XAPlusTLog.class);

    private final String serverId;
    private final XAPlusEngine engine;

    XAPlusTLog(String serverId, XAPlusEngine engine) {
        this.serverId = serverId;
        this.engine = engine;
    }

    Boolean findTransactionStatus(XAPlusUid gtrid) throws SQLException {
        DataSource tlogDataSource = engine.getTLogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(SELECT_SQL)) {
                statement.setBytes(1, gtrid.getArray());
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        boolean tStatus = resultSet.getBoolean(1);
                        if (logger.isTraceEnabled()) {
                            logger.trace("Transaction status found, status={}, gtrid={}", tStatus, gtrid);
                        }
                        return tStatus;
                    } else {
                        if (logger.isTraceEnabled()) {
                            logger.trace("Transaction status not found, gtrid={}", gtrid);
                        }
                        // If not found, then return rollback status
                        return false;
                    }
                }
            }
        }
    }

    void logCommitDecision(XAPlusUid gtrid) throws SQLException {
        log(gtrid, true);
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
