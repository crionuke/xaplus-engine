package com.crionuke.xaplus;

import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

@Component
class XAPlusTLog {

    // Commit, rollback and done statuses
    enum TSTATUS {
        C, R, D
    }

    private final XAPlusProperties properties;
    private final XAPlusEngine engine;

    XAPlusTLog(XAPlusProperties properties, XAPlusEngine engine) {
        this.properties = properties;
        this.engine = engine;
    }

    void log(XAPlusXid xid, String uniqueName, TSTATUS tstatus) throws SQLException {
        Map<XAPlusXid, String> uniqueNames = new HashMap<>();
        uniqueNames.put(xid, uniqueName);
        log(uniqueNames, tstatus);
    }

    void log(Map<XAPlusXid, String> uniqueNames, TSTATUS tstatus) throws SQLException {
        DataSource tlogDataSource = engine.getTlogDataSource();
        try (Connection connection = tlogDataSource.getConnection()) {
            String sql = "INSERT INTO tlog (t_timestamp, t_server_id, t_gtrid, t_bqual, t_unique_name, t_status) "
                    + "VALUES(?, ?, ?, ?, ?, ?)";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setTimestamp(1, new Timestamp(System.currentTimeMillis()));
                statement.setString(2, properties.getServerId());
                statement.setString(6, tstatus.name());
                for (Map.Entry<XAPlusXid, String> entry : uniqueNames.entrySet()) {
                    XAPlusXid branchXid = entry.getKey();
                    XAPlusUid branchGtrid = branchXid.getGlobalTransactionIdUid();
                    XAPlusUid branchBqual = branchXid.getBranchQualifierUid();
                    String uniqueName = entry.getValue();
                    statement.setBytes(3, branchGtrid.getArray());
                    statement.setBytes(4, branchBqual.getArray());
                    statement.setString(5, uniqueName);
                    statement.addBatch();
                }
                statement.executeBatch();
            }
        }
    }
}
