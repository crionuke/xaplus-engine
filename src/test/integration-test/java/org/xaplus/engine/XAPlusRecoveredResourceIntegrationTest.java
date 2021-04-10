package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.sql.SQLException;
import java.util.Set;

public class XAPlusRecoveredResourceIntegrationTest extends XAPlusIntegrationTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveredResourceIntegrationTest.class);

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
    }

    @Test
    public void testRecovery() throws SQLException, XAException {
        XADataSource xaDataSource = createXADataSource();
        cleanUpXAResource(xaDataSource.getXAConnection().getXAResource());
        // First server
        XAPlusTestTransaction transaction1 = new XAPlusTestTransaction(xaDataSource, XA_PLUS_RESOURCE_1);
        prepareTransaction(transaction1);
        logger.info("{}", transaction1.getBranchXid());
        // Second server
        XAPlusTestTransaction transaction21 = new XAPlusTestTransaction(xaDataSource, XA_PLUS_RESOURCE_2);
        prepareTransaction(transaction21);
        logger.info("{}", transaction21.getBranchXid());
        XAPlusTestTransaction transaction22 = new XAPlusTestTransaction(xaDataSource, XA_PLUS_RESOURCE_2);
        prepareTransaction(transaction22);
        logger.info("{}", transaction22.getBranchXid());
        // Third server
        XAPlusTestTransaction transaction31 = new XAPlusTestTransaction(xaDataSource, XA_PLUS_RESOURCE_3);
        prepareTransaction(transaction31);
        logger.info("{}", transaction31.getBranchXid());
        XAPlusTestTransaction transaction32 = new XAPlusTestTransaction(xaDataSource, XA_PLUS_RESOURCE_3);
        prepareTransaction(transaction32);
        logger.info("{}", transaction32.getBranchXid());
        XAPlusTestTransaction transaction33 = new XAPlusTestTransaction(xaDataSource, XA_PLUS_RESOURCE_3);
        prepareTransaction(transaction33);
        logger.info("{}", transaction33.getBranchXid());
        // Recovery
        testRecoveryFor(XA_PLUS_RESOURCE_1,
                new XAPlusXid[]{transaction1.getBranchXid()});
        testRecoveryFor(XA_PLUS_RESOURCE_2,
                new XAPlusXid[]{transaction21.getBranchXid(), transaction22.getBranchXid()});
        testRecoveryFor(XA_PLUS_RESOURCE_3,
                new XAPlusXid[]{transaction31.getBranchXid(),
                        transaction32.getBranchXid(),
                        transaction33.getBranchXid()});
    }

    private void prepareTransaction(XAPlusTestTransaction transaction) throws XAException, SQLException {
        transaction.start();
        transaction.insert();
        transaction.end();
        transaction.prepare();
        transaction.close();
    }

    private void testRecoveryFor(String serverId, XAPlusXid[] expectedXids) throws XAException, SQLException {
        long inFlightCutoff = System.currentTimeMillis();
        XAPlusRecoveredResource xaPlusRecoveredResource =
                new XAPlusRecoveredResource(XA_RESOURCE_1, serverId, inFlightCutoff, createXADataSource().getXAConnection());
        int xidCount = xaPlusRecoveredResource.recovery();
        logger.info("{} xid recovered for {}", xidCount, serverId);
        Set<XAPlusXid> recoveredXids = xaPlusRecoveredResource.getRecoveredXids();
        logger.info("{}", recoveredXids);
        for (XAPlusXid xid : expectedXids) {
            assertTrue(recoveredXids.contains(xid));
        }
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
