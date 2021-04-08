package org.xaplus.engine;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class XAPlusRecoverIntegrationTest extends XAPlusIntegrationTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoverIntegrationTest.class);

    TestResource testResource;

    @Before
    public void beforeTest() throws SQLException, XAException {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        createXADataSource();
        testResource = new TestResource();
        cleanUpXAResource(testResource.getXaResource());
    }

    @After
    public void afterTest() throws Exception {
        testResource.close();
    }

    @Test
    public void testRecovery() throws Exception {
        int count = 10;
        List<XAPlusXid> danglingByServer1 = new ArrayList<>();
        List<XAPlusXid> danglingByServer2 = new ArrayList<>();
        List<XAPlusXid> danglingByServer3 = new ArrayList<>();
        List<OneBranchTransaction> transactions = new ArrayList<>();
        // Transactions by SERVER_ID_1
        for (int i = 0; i < count; i++) {
            OneBranchTransaction transaction = new OneBranchTransaction(XA_PLUS_RESOURCE_1);
            danglingByServer1.add(transaction.getBranchXid());
            transactions.add(transaction);
        }
        // Transactions by SERVER_ID_2
        for (int i = 0; i < count; i++) {
            OneBranchTransaction transaction = new OneBranchTransaction(XA_PLUS_RESOURCE_2);
            danglingByServer2.add(transaction.getBranchXid());
            transactions.add(transaction);
        }
        // Transactions by SERVER_ID_3
        for (int i = 0; i < count; i++) {
            OneBranchTransaction transaction = new OneBranchTransaction(XA_PLUS_RESOURCE_3);
            danglingByServer3.add(transaction.getBranchXid());
            transactions.add(transaction);
        }
        // Make all transactions dangling
        for (OneBranchTransaction transaction : transactions) {
            prepareTransaction(transaction);
            transaction.close();
        }
        testRecoveryFor(XA_PLUS_RESOURCE_1, danglingByServer1);
        testRecoveryFor(XA_PLUS_RESOURCE_2, danglingByServer2);
        testRecoveryFor(XA_PLUS_RESOURCE_3, danglingByServer3);
    }

    private void testRecoveryFor(String serverId, List<XAPlusXid> expectedXids) throws XAException {
//        XAPlusRecover xaPlusRecover =
//                new XAPlusRecover(serverId, testResource.getXaResource());
//        Set<XAPlusXid> recoveredXids = xaPlusRecover.recovery();
//        logger.info("Recovery {} xids by {}", recoveredXids.size(), serverId);
//        for (XAPlusXid recoveredXid : recoveredXids) {
//            assertTrue(expectedXids.contains(recoveredXid));
//        }
//        assertEquals(expectedXids.size(), recoveredXids.size());
    }

    private void prepareTransaction(OneBranchTransaction transaction) throws XAException, SQLException {
        transaction.start();
        transaction.insert();
        transaction.end();
        transaction.prepare();
    }

    private void cleanUpXAResource(XAResource xaResource) throws XAException {
        Xid[] recovered = xaResource.recover(XAResource.TMSTARTRSCAN);
        logger.info("Clean up {} xids from XAResource", recovered.length);
        for (Xid xid : recovered) {
            try {
                xaResource.rollback(xid);
            } catch (XAException e) {
                continue;
            }
        }
    }
}
