package com.crionuke.xaplus;

import com.opentable.db.postgres.embedded.DatabaseConnectionPreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

public class XAPlusTLogTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTLogTest.class);

    @Rule
    public PreparedDbRule dbRule = EmbeddedPostgresRules.preparedDatabase(new TlogPreparer());

    XAPlusTLog tLog;

    @Before
    public void beforeTest() {
        createXAPlusComponents();
        engine.setTLogDataSource(dbRule.getTestDatabase());
        tLog = new XAPlusTLog(properties, engine);
    }

    @Test
    public void testOneLog() throws SQLException {
        XAPlusXid xid = generateSuperiorXid();
        tLog.log(xid, XA_RESOURCE_1, XAPlusTLog.TSTATUS.C);
        try (Connection connection = engine.getTlogDataSource().getConnection()) {
            String sql = "SELECT t_server_id, t_gtrid, t_bqual, t_unique_name, t_status FROM tlog";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    resultSet.next();
                    String serverId = resultSet.getString(1);
                    byte[] gtridBytes = resultSet.getBytes(2);
                    XAPlusUid gtridUid = new XAPlusUid(gtridBytes);
                    byte[] bqualBytes = resultSet.getBytes(3);
                    XAPlusUid bqualUid = new XAPlusUid(bqualBytes);
                    String uniqueName = resultSet.getString(4);
                    String status = resultSet.getString(5);
                    logger.info("serverId={}, uniqueName={}, status={}", serverId, uniqueName, status);
                    // Check
                    assertEquals(SERVER_ID, serverId);
                    assertEquals(xid.getGlobalTransactionIdUid(), gtridUid);
                    assertEquals(xid.getBranchQualifierUid(), bqualUid);
                    assertEquals(XA_RESOURCE_1, uniqueName);
                    assertEquals(XAPlusTLog.TSTATUS.C.toString(), status);
                }
            }
        }
    }

    @Test
    public void testTransactionLog() throws SQLException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Map<XAPlusXid, String> uniqueNames = transaction.getUniqueNames();
        tLog.log(transaction, XAPlusTLog.TSTATUS.C);
        try (Connection connection = engine.getTlogDataSource().getConnection()) {
            String sql = "SELECT t_server_id, t_gtrid, t_bqual, t_unique_name, t_status FROM tlog";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    int count = 0;
                    while (resultSet.next()) {
                        String serverId = resultSet.getString(1);
                        byte[] gtridBytes = resultSet.getBytes(2);
                        XAPlusUid gtridUid = new XAPlusUid(gtridBytes);
                        byte[] bqualBytes = resultSet.getBytes(3);
                        XAPlusUid bqualUid = new XAPlusUid(bqualBytes);
                        String uniqueName = resultSet.getString(4);
                        String status = resultSet.getString(5);
                        count++;
                        logger.info("serverId={}, uniqueName={}, status={}", serverId, uniqueName, status);
                        // Checks
                        assertEquals(SERVER_ID, serverId);
                        XAPlusXid xid = new XAPlusXid(gtridUid, bqualUid);
                        assertTrue(uniqueNames.containsKey(xid));
                        assertTrue(uniqueNames.containsValue(uniqueName));
                        assertEquals(XAPlusTLog.TSTATUS.C.toString(), status);
                    }
                    assertEquals(uniqueNames.size(), count);
                }
            }
        }
    }

    private class TlogPreparer implements DatabaseConnectionPreparer {

        @Override
        public void prepare(Connection conn) throws SQLException {
            String sql = "CREATE TABLE tlog (t_id bigserial PRIMARY KEY, " +
                    "t_timestamp timestamp NOT NULL, " +
                    "t_server_id varchar(64) NOT NULL, " +
                    "t_gtrid bytea, " +
                    "t_bqual bytea, " +
                    "t_unique_name varchar(64) NOT NULL, " +
                    "t_status varchar(1) NOT NULL);";
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.execute();
            }
        }
    }
}
