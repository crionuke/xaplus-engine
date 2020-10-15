package org.xaplus.engine;

import com.opentable.db.postgres.embedded.DatabaseConnectionPreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class XAPlusTLogTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTLogTest.class);

    @Rule
    public PreparedDbRule preparedDbRule = EmbeddedPostgresRules.preparedDatabase(new TlogPreparer("tlog.sql"));

    XAPlusTLog tLog;

    @Before
    public void beforeTest() {
        createXAPlusComponents(SERVER_ID_DEFAULT);
        engine.setTLogDataSource(preparedDbRule.getTestDatabase());
        tLog = new XAPlusTLog(properties, engine);
    }

    @Test
    public void testLogCommitXidDecision() throws SQLException {
        XAPlusXid xid = generateSuperiorXid();
        tLog.logCommitXidDecision(xid, XA_RESOURCE_1);
        TLogRecord record = selectTLogRecords().get(0);
        assertNotNull(record);
        assertEquals(properties.getServerId(), record.getServerId());
        assertEquals(xid, record.getXid());
        assertEquals(XA_RESOURCE_1, record.getUniqueName());
        assertEquals(true, record.getTStatus());
        assertEquals(false, record.isComplete());
    }

    @Test
    public void testLogXidCommitted() throws SQLException {
        XAPlusXid xid = generateSuperiorXid();
        tLog.logXidCommitted(xid, XA_RESOURCE_1);
        TLogRecord record = selectTLogRecords().get(0);
        assertNotNull(record);
        assertEquals(properties.getServerId(), record.getServerId());
        assertEquals(xid, record.getXid());
        assertEquals(XA_RESOURCE_1, record.getUniqueName());
        assertEquals(true, record.getTStatus());
        assertEquals(true, record.isComplete());
    }

    @Test
    public void testLogRollbackXidDecision() throws SQLException {
        XAPlusXid xid = generateSuperiorXid();
        tLog.logRollbackXidDecision(xid, XA_RESOURCE_1);
        TLogRecord record = selectTLogRecords().get(0);
        assertNotNull(record);
        assertEquals(properties.getServerId(), record.getServerId());
        assertEquals(xid, record.getXid());
        assertEquals(XA_RESOURCE_1, record.getUniqueName());
        assertEquals(false, record.getTStatus());
        assertEquals(false, record.isComplete());
    }

    @Test
    public void testLogXidRolledBack() throws SQLException {
        XAPlusXid xid = generateSuperiorXid();
        tLog.logXidRolledBack(xid, XA_RESOURCE_1);
        TLogRecord record = selectTLogRecords().get(0);
        assertNotNull(record);
        assertEquals(properties.getServerId(), record.getServerId());
        assertEquals(xid, record.getXid());
        assertEquals(XA_RESOURCE_1, record.getUniqueName());
        assertEquals(false, record.getTStatus());
        assertEquals(true, record.isComplete());
    }

    @Test
    public void testLogCommitTransactionDecision() throws SQLException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Map<XAPlusXid, String> uniqueNames = transaction.getUniqueNames();
        tLog.logCommitTransactionDecision(transaction);
        List<TLogRecord> records = selectTLogRecords();
        for (TLogRecord record : records) {
            assertEquals(properties.getServerId(), record.getServerId());
            assertNotNull(uniqueNames.get(record.getXid()));
            assertEquals(uniqueNames.get(record.getXid()), record.getUniqueName());
            assertEquals(true, record.getTStatus());
            assertEquals(false, record.isComplete());
        }
        assertEquals(uniqueNames.size(), records.size());
    }

    @Test
    public void testLogTransactionCommitted() throws SQLException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Map<XAPlusXid, String> uniqueNames = transaction.getUniqueNames();
        tLog.logTransactionCommitted(transaction);
        List<TLogRecord> records = selectTLogRecords();
        for (TLogRecord record : records) {
            assertEquals(properties.getServerId(), record.getServerId());
            assertNotNull(uniqueNames.get(record.getXid()));
            assertEquals(uniqueNames.get(record.getXid()), record.getUniqueName());
            assertEquals(true, record.getTStatus());
            assertEquals(true, record.isComplete());
        }
        assertEquals(uniqueNames.size(), records.size());
    }

    @Test
    public void testLogRollbackTransactionDecision() throws SQLException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Map<XAPlusXid, String> uniqueNames = transaction.getUniqueNames();
        tLog.logRollbackTransactionDecision(transaction);
        List<TLogRecord> records = selectTLogRecords();
        for (TLogRecord record : records) {
            assertEquals(properties.getServerId(), record.getServerId());
            assertNotNull(uniqueNames.get(record.getXid()));
            assertEquals(uniqueNames.get(record.getXid()), record.getUniqueName());
            assertEquals(false, record.getTStatus());
            assertEquals(false, record.isComplete());
        }
        assertEquals(uniqueNames.size(), records.size());
    }

    @Test
    public void testTransactionRolledBack() throws SQLException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Map<XAPlusXid, String> uniqueNames = transaction.getUniqueNames();
        tLog.logTransactionRolledBack(transaction);
        List<TLogRecord> records = selectTLogRecords();
        for (TLogRecord record : records) {
            assertEquals(properties.getServerId(), record.getServerId());
            assertNotNull(uniqueNames.get(record.getXid()));
            assertEquals(uniqueNames.get(record.getXid()), record.getUniqueName());
            assertEquals(false, record.getTStatus());
            assertEquals(true, record.isComplete());
        }
        assertEquals(uniqueNames.size(), records.size());
    }

    @Test
    public void testFindDanglingTransactions() throws SQLException {
        XAPlusTransaction transaction1 = createTestSuperiorTransaction();
        XAPlusTransaction transaction2 = createTestSuperiorTransaction();
        XAPlusTransaction transaction3 = createTestSuperiorTransaction();
        Map<XAPlusXid, String> uniqueNames2 = transaction2.getUniqueNames();
        tLog.logCommitTransactionDecision(transaction1);
        tLog.logRollbackTransactionDecision(transaction2);
        tLog.logTransactionCommitted(transaction1);
        tLog.logRollbackTransactionDecision(transaction3);
        tLog.logTransactionRolledBack(transaction3);
        Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = tLog.findDanglingTransactions();
        for (Map.Entry<String, Map<XAPlusXid, Boolean>> entry : danglingTransactions.entrySet()) {
            String uniqueName = entry.getKey();
            Map<XAPlusXid, Boolean> branches = entry.getValue();
            assertTrue(uniqueNames2.containsValue(uniqueName));
            for (XAPlusXid branchXid : branches.keySet()) {
                assertTrue(uniqueNames2.containsKey(branchXid));
            }
        }
        assertEquals(danglingTransactions.size(), uniqueNames2.size());
    }

    List<TLogRecord> selectTLogRecords() throws SQLException {
        List<TLogRecord> records = new ArrayList<>();
        try (Connection connection = engine.getTlogDataSource().getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(XAPlusTLog.SELECT_SQL)) {
                try (ResultSet resultSet = statement.executeQuery()) {
                    while(resultSet.next()) {
                        String serverId = resultSet.getString(1);
                        byte[] gtridBytes = resultSet.getBytes(2);
                        XAPlusUid gtridUid = new XAPlusUid(gtridBytes);
                        byte[] bqualBytes = resultSet.getBytes(3);
                        XAPlusUid bqualUid = new XAPlusUid(bqualBytes);
                        String uniqueName = resultSet.getString(4);
                        boolean status = resultSet.getBoolean(5);
                        boolean complete = resultSet.getBoolean(6);
                        records.add(new TLogRecord(serverId, new XAPlusXid(gtridUid, bqualUid),
                                uniqueName, status, complete));
                    }
                }
            }
        }
        return records;
    }

    private class TlogPreparer implements DatabaseConnectionPreparer {

        private final String sql;

        TlogPreparer(String initScript) {
            StringBuilder lines = new StringBuilder();
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(getClass().getClassLoader().getResourceAsStream(initScript)));
            try {
                String line = reader.readLine();
                while (line != null) {
                    lines.append(line).append("\n");
                    line = reader.readLine();
                }
            } catch (IOException e) {
            }
            sql = lines.toString();
        }

        @Override
        public void prepare(Connection conn) throws SQLException {
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.execute();
            }
        }
    }

    private class TLogRecord {
        private final String serverId;
        private final Xid xid;
        private final String uniqueName;
        private final boolean tstatus;
        private final boolean complete;

        TLogRecord(String serverId, Xid xid, String uniqueName, boolean tstatus, boolean complete) {
            this.serverId = serverId;
            this.xid = xid;
            this.uniqueName = uniqueName;
            this.tstatus = tstatus;
            this.complete = complete;
        }

        String getServerId() {
            return serverId;
        }

        Xid getXid() {
            return xid;
        }

        String getUniqueName() {
            return uniqueName;
        }

        boolean getTStatus() {
            return tstatus;
        }

        boolean isComplete() {
            return complete;
        }
    }
}
