package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class XAPlusTLogTest extends XAPlusIntegrationTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTLogTest.class);

    private XAPlusTLog tLog;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        engine.setTLogDataSource(createTLogDataSource());
        tLog = new XAPlusTLog(properties.getServerId(), engine);
    }

    @Test
    public void testFindTransactionStatus() throws SQLException {
        XAPlusUid gtrid11 = new XAPlusUid(XA_PLUS_RESOURCE_1);
        XAPlusUid gtrid12 = new XAPlusUid(XA_PLUS_RESOURCE_1);
        XAPlusUid gtrid21 = new XAPlusUid(XA_PLUS_RESOURCE_2);
        XAPlusUid gtrid22 = new XAPlusUid(XA_PLUS_RESOURCE_2);
        XAPlusUid gtrid31 = new XAPlusUid(XA_PLUS_RESOURCE_3);
        XAPlusUid gtrid32 = new XAPlusUid(XA_PLUS_RESOURCE_3);
        tLog.logCommitDecision(gtrid11);
        tLog.logCommitDecision(gtrid12);
        tLog.logRollbackDecision(gtrid21);
        tLog.logRollbackDecision(gtrid22);
        tLog.logCommitDecision(gtrid31);
        tLog.logRollbackDecision(gtrid32);
        assertTrue(tLog.findTransactionStatus(gtrid11));
        assertTrue(tLog.findTransactionStatus(gtrid12));
        assertFalse(tLog.findTransactionStatus(gtrid21));
        assertFalse(tLog.findTransactionStatus(gtrid22));
        assertTrue(tLog.findTransactionStatus(gtrid31));
        assertFalse(tLog.findTransactionStatus(gtrid32));
    }
}
