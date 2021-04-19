package org.xaplus.engine;

import org.junit.Assert;
import org.xaplus.engine.stubs.*;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class XAPlusUnitTest extends Assert {
    protected final int QUEUE_SIZE = 128;
    protected final int DEFAULT_TIMEOUT_S = 2;
    protected final int POLL_TIMIOUT_MS = 2000;
    protected final int VERIFY_MS = 1000;

    protected final String XA_RESOURCE_1 = "db-1-stub";
    protected final String XA_RESOURCE_2 = "db-2-stub";
    protected final String XA_RESOURCE_3 = "db-3-stub";

    protected final String XA_PLUS_RESOURCE_1 = "server-1-stub";
    protected final String XA_PLUS_RESOURCE_2 = "server-2-stub";
    protected final String XA_PLUS_RESOURCE_3 = "server-3-stub";

    protected XAPlusProperties properties;
    protected XAPlusResources resources;
    protected XAPlusThreadOfControl threadOfControl;
    protected XAPlusEngine engine;
    protected XAPlusThreadPool threadPool;
    protected XAPlusDispatcher dispatcher;

    protected void createXAPlusComponents(String serverId) {
        createXAPlusComponents(serverId, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);
    }

    protected void createXAPlusComponents(String serverId, int transactionsTimeoutInSeconds, int recoveryTimeoutInSeconds) {
        properties = new XAPlusProperties(serverId, QUEUE_SIZE, transactionsTimeoutInSeconds, recoveryTimeoutInSeconds);
        resources = new XAPlusResources();
        resources.register(new XADataSourceStub(), XA_RESOURCE_1);
        resources.register(new XADataSourceStub(), XA_RESOURCE_2);
        resources.register(new XADataSourceStub(), XA_RESOURCE_3);
        resources.register(new XAPlusFactoryStub(), XA_PLUS_RESOURCE_1);
        resources.register(new XAPlusFactoryStub(), XA_PLUS_RESOURCE_2);
        resources.register(new XAPlusFactoryStub(), XA_PLUS_RESOURCE_3);
        threadOfControl = new XAPlusThreadOfControl();
        engine = new XAPlusEngine(properties, dispatcher, resources, threadOfControl);
        threadPool = new XAPlusThreadPool();
        dispatcher = new XAPlusDispatcher();
    }

    protected XAPlusTransaction createTransaction(String gtridServerId, String bqualServerId) {
        XAPlusXid xid = new XAPlusXid(new XAPlusUid(gtridServerId), new XAPlusUid(bqualServerId));
        XAPlusTransaction transaction = new XAPlusTransaction(xid, properties.getTransactionsTimeoutInSeconds(),
                properties.getServerId());
        return transaction;
    }

    protected XAPlusXid createJdbcXid(XAPlusTransaction transaction) {
        return new XAPlusXid(transaction.getXid().getGtrid(), properties.getServerId());
    }

    protected XAPlusXid createXAPlusXid(XAPlusTransaction transaction, String serverId) {
        return new XAPlusXid(transaction.getXid().getGtrid(), serverId);
    }

    protected XAPlusTransaction createTestSuperiorTransaction() throws SQLException, XAException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createJdbcXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAConnectionStub());
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAConnectionStub());
        XAPlusXid bxid3 = createJdbcXid(transaction);
        transaction.enlist(bxid3, XA_RESOURCE_3, new XAConnectionStub());
        XAPlusXid bxid4 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_2);
        transaction.enlist(bxid4, XA_PLUS_RESOURCE_2, new XAPlusResourceStub());
        XAPlusXid bxid5 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_3);
        transaction.enlist(bxid5, XA_PLUS_RESOURCE_3, new XAPlusResourceStub());
        return transaction;
    }

    class TestSuperiorDataSet {

        final TestSuperiorTransaction transaction1;
        final TestSuperiorTransaction transaction2;
        final TestSuperiorTransaction transaction3;

        final Set<XAPlusXid> xaResource1PreparedXids;
        final Set<XAPlusXid> xaResource2PreparedXids;
        final Set<XAPlusXid> xaResource3PreparedXids;

        final Map<String, Set<XAPlusXid>> allPreparedXids;

        final Map<String, Map<XAPlusXid, Boolean>> xaDanglingTransactions;
        final Map<String, Map<XAPlusXid, Boolean>> xaPlusDanglingTransactions;
        final Map<String, Map<XAPlusXid, Boolean>> allDanglingTransactions;

        final Map<XAPlusXid, Boolean> xaDanglingXids;
        final Map<XAPlusXid, Boolean> xaPlusDanglingXids;

        final Set<XAPlusXid> xaNoDecisionXids;

        TestSuperiorDataSet(String serverId) {
            transaction1 = new TestSuperiorTransaction(serverId);
            transaction2 = new TestSuperiorTransaction(serverId);
            transaction3 = new TestSuperiorTransaction(serverId);

            xaResource1PreparedXids = new HashSet<>();
            xaResource1PreparedXids.addAll(transaction1.xaResource1PreparedXids);
            xaResource1PreparedXids.addAll(transaction2.xaResource1PreparedXids);
            xaResource1PreparedXids.addAll(transaction3.xaResource1PreparedXids);

            xaResource2PreparedXids = new HashSet<>();
            xaResource2PreparedXids.addAll(transaction1.xaResource2PreparedXids);
            xaResource2PreparedXids.addAll(transaction2.xaResource2PreparedXids);
            xaResource2PreparedXids.addAll(transaction3.xaResource2PreparedXids);

            xaResource3PreparedXids = new HashSet<>();
            xaResource3PreparedXids.addAll(transaction1.xaResource3PreparedXids);
            xaResource3PreparedXids.addAll(transaction2.xaResource3PreparedXids);
            xaResource3PreparedXids.addAll(transaction3.xaResource3PreparedXids);

            allPreparedXids = new HashMap<>();
            allPreparedXids.put(XA_RESOURCE_1, xaResource1PreparedXids);
            allPreparedXids.put(XA_RESOURCE_2, xaResource2PreparedXids);
            allPreparedXids.put(XA_RESOURCE_3, xaResource3PreparedXids);

            Map<XAPlusXid, Boolean> xaResource1AllDanglingXids = new HashMap<>();
            xaResource1AllDanglingXids.putAll(transaction1.xaResource1DanglingXids);
            xaResource1AllDanglingXids.putAll(transaction2.xaResource1DanglingXids);
            xaResource1AllDanglingXids.putAll(transaction3.xaResource1DanglingXids);

            Map<XAPlusXid, Boolean> xaResource2AllDanglingXids = new HashMap<>();
            xaResource2AllDanglingXids.putAll(transaction1.xaResource2DanglingXids);
            xaResource2AllDanglingXids.putAll(transaction2.xaResource2DanglingXids);
            xaResource2AllDanglingXids.putAll(transaction3.xaResource2DanglingXids);

            Map<XAPlusXid, Boolean> xaResource3AllDanglingXids = new HashMap<>();
            xaResource3AllDanglingXids.putAll(transaction1.xaResource3DanglingXids);
            xaResource3AllDanglingXids.putAll(transaction2.xaResource3DanglingXids);
            xaResource3AllDanglingXids.putAll(transaction3.xaResource3DanglingXids);

            Map<XAPlusXid, Boolean> xaPlusResource2AllDanglingXids = new HashMap<>();
            xaPlusResource2AllDanglingXids.putAll(transaction1.xaPlusResource2DanglingXids);
            xaPlusResource2AllDanglingXids.putAll(transaction2.xaPlusResource2DanglingXids);

            Map<XAPlusXid, Boolean> xaPlusResource3AllDanglingXids = new HashMap<>();
            xaPlusResource3AllDanglingXids.putAll(transaction1.xaPlusResource3DanglingXids);
            xaPlusResource3AllDanglingXids.putAll(transaction2.xaPlusResource3DanglingXids);

            xaDanglingTransactions = new HashMap<>();
            xaDanglingTransactions.put(XA_RESOURCE_1, xaResource1AllDanglingXids);
            xaDanglingTransactions.put(XA_RESOURCE_2, xaResource2AllDanglingXids);
            xaDanglingTransactions.put(XA_RESOURCE_3, xaResource3AllDanglingXids);

            xaPlusDanglingTransactions = new HashMap<>();
            xaPlusDanglingTransactions.put(XA_PLUS_RESOURCE_2, xaPlusResource2AllDanglingXids);
            xaPlusDanglingTransactions.put(XA_PLUS_RESOURCE_3, xaPlusResource3AllDanglingXids);

            allDanglingTransactions = new HashMap<>();
            allDanglingTransactions.putAll(xaDanglingTransactions);
            allDanglingTransactions.putAll(xaPlusDanglingTransactions);

            xaDanglingXids = new HashMap<>();
            xaDanglingXids.putAll(xaResource1AllDanglingXids);
            xaDanglingXids.putAll(xaResource2AllDanglingXids);
            xaDanglingXids.putAll(xaResource3AllDanglingXids);

            xaPlusDanglingXids = new HashMap<>();
            xaPlusDanglingXids.putAll(xaPlusResource2AllDanglingXids);
            xaPlusDanglingXids.putAll(xaPlusResource3AllDanglingXids);

            Set<XAPlusXid> xaResource1NoDecisionXids = new HashSet<>();
            xaResource1NoDecisionXids.addAll(transaction1.xaResource1NoDecisionXids);
            xaResource1NoDecisionXids.addAll(transaction2.xaResource1NoDecisionXids);
            xaResource1NoDecisionXids.addAll(transaction3.xaResource1NoDecisionXids);

            Set<XAPlusXid> xaResource2NoDecisionXids = new HashSet<>();
            xaResource2NoDecisionXids.addAll(transaction1.xaResource2NoDecisionXids);
            xaResource2NoDecisionXids.addAll(transaction2.xaResource2NoDecisionXids);
            xaResource2NoDecisionXids.addAll(transaction3.xaResource2NoDecisionXids);

            Set<XAPlusXid> xaResource3NoDecisionXids = new HashSet<>();
            xaResource3NoDecisionXids.addAll(transaction1.xaResource3NoDecisionXids);
            xaResource3NoDecisionXids.addAll(transaction2.xaResource3NoDecisionXids);
            xaResource3NoDecisionXids.addAll(transaction3.xaResource3NoDecisionXids);

            xaNoDecisionXids = new HashSet<>();
            xaNoDecisionXids.addAll(xaResource1NoDecisionXids);
            xaNoDecisionXids.addAll(xaResource2NoDecisionXids);
            xaNoDecisionXids.addAll(xaResource3NoDecisionXids);
        }
    }

    class TestSubordinateDataSet {

        final TestSubordinateTransaction transaction1;
        final TestSubordinateTransaction transaction2;

        final Set<XAPlusXid> xaResource1PreparedXids;
        final Set<XAPlusXid> xaResource2PreparedXids;
        final Set<XAPlusXid> xaResource3PreparedXids;

        final Map<String, Set<XAPlusXid>> allPreparedXids;

        final Map<String, Map<XAPlusXid, Boolean>> xaDanglingTransactions;
        final Map<String, Map<XAPlusXid, Boolean>> allDanglingTransactions;

        final Map<XAPlusXid, Boolean> xaDanglingXids;

        final Set<XAPlusXid> xaNoDecisionXids;

        TestSubordinateDataSet(String gtridServerId1, String gtridServerId2, String bqualServerId) {
            transaction1 = new TestSubordinateTransaction(gtridServerId1, bqualServerId);
            transaction2 = new TestSubordinateTransaction(gtridServerId2, bqualServerId);

            xaResource1PreparedXids = new HashSet<>();
            xaResource1PreparedXids.addAll(transaction1.xaResource1PreparedXids);
            xaResource1PreparedXids.addAll(transaction2.xaResource1PreparedXids);

            xaResource2PreparedXids = new HashSet<>();
            xaResource2PreparedXids.addAll(transaction1.xaResource2PreparedXids);
            xaResource2PreparedXids.addAll(transaction2.xaResource2PreparedXids);

            xaResource3PreparedXids = new HashSet<>();
            xaResource3PreparedXids.addAll(transaction1.xaResource3PreparedXids);
            xaResource3PreparedXids.addAll(transaction2.xaResource3PreparedXids);

            allPreparedXids = new HashMap<>();
            allPreparedXids.put(XA_RESOURCE_1, xaResource1PreparedXids);
            allPreparedXids.put(XA_RESOURCE_2, xaResource2PreparedXids);
            allPreparedXids.put(XA_RESOURCE_3, xaResource3PreparedXids);

            Map<XAPlusXid, Boolean> xaResource1AllDanglingXids = new HashMap<>();
            xaResource1AllDanglingXids.putAll(transaction1.xaResource1DanglingXids);
            xaResource1AllDanglingXids.putAll(transaction2.xaResource1DanglingXids);

            Map<XAPlusXid, Boolean> xaResource2AllDanglingXids = new HashMap<>();
            xaResource2AllDanglingXids.putAll(transaction1.xaResource2DanglingXids);
            xaResource2AllDanglingXids.putAll(transaction2.xaResource2DanglingXids);

            Map<XAPlusXid, Boolean> xaResource3AllDanglingXids = new HashMap<>();
            xaResource3AllDanglingXids.putAll(transaction1.xaResource3DanglingXids);
            xaResource3AllDanglingXids.putAll(transaction2.xaResource3DanglingXids);

            xaDanglingTransactions = new HashMap<>();
            xaDanglingTransactions.put(XA_RESOURCE_1, xaResource1AllDanglingXids);
            xaDanglingTransactions.put(XA_RESOURCE_2, xaResource2AllDanglingXids);
            xaDanglingTransactions.put(XA_RESOURCE_3, xaResource3AllDanglingXids);

            allDanglingTransactions = new HashMap<>();
            allDanglingTransactions.putAll(xaDanglingTransactions);

            xaDanglingXids = new HashMap<>();
            xaDanglingXids.putAll(xaResource1AllDanglingXids);
            xaDanglingXids.putAll(xaResource2AllDanglingXids);
            xaDanglingXids.putAll(xaResource3AllDanglingXids);

            Set<XAPlusXid> xaResource1NoDecisionXids = new HashSet<>();
            xaResource1NoDecisionXids.addAll(transaction1.xaResource1NoDecisionXids);
            xaResource1NoDecisionXids.addAll(transaction2.xaResource1NoDecisionXids);

            Set<XAPlusXid> xaResource2NoDecisionXids = new HashSet<>();
            xaResource2NoDecisionXids.addAll(transaction1.xaResource2NoDecisionXids);
            xaResource2NoDecisionXids.addAll(transaction2.xaResource2NoDecisionXids);

            Set<XAPlusXid> xaResource3NoDecisionXids = new HashSet<>();
            xaResource3NoDecisionXids.addAll(transaction1.xaResource3NoDecisionXids);
            xaResource3NoDecisionXids.addAll(transaction2.xaResource3NoDecisionXids);

            xaNoDecisionXids = new HashSet<>();
            xaNoDecisionXids.addAll(xaResource1NoDecisionXids);
            xaNoDecisionXids.addAll(xaResource2NoDecisionXids);
            xaNoDecisionXids.addAll(xaResource3NoDecisionXids);
        }
    }

    class TestSuperiorTransaction extends XAPlusBranches {

        TestSuperiorTransaction(String serverId) {
            super(createTransaction(serverId, serverId));
        }
    }

    class TestSubordinateTransaction extends XABranches {

        TestSubordinateTransaction(String superiorServerId, String subordinateServerId) {
            super(createTransaction(superiorServerId, subordinateServerId));
        }
    }

    class XABranches {
        final XAPlusTransaction transaction;

        final Set<XAPlusXid> xaResource1PreparedXids;
        final Map<XAPlusXid, Boolean> xaResource1DanglingXids;
        final Set<XAPlusXid> xaResource1NoDecisionXids;
        final Set<XAPlusXid> xaResource2PreparedXids;
        final Map<XAPlusXid, Boolean> xaResource2DanglingXids;
        final Set<XAPlusXid> xaResource2NoDecisionXids;
        final Set<XAPlusXid> xaResource3PreparedXids;
        final Map<XAPlusXid, Boolean> xaResource3DanglingXids;
        final Set<XAPlusXid> xaResource3NoDecisionXids;

        XABranches(XAPlusTransaction transaction) {
            this.transaction = transaction;

            xaResource1PreparedXids = new HashSet<>();
            xaResource1DanglingXids = new HashMap<>();
            xaResource1NoDecisionXids = new HashSet<>();
            XAPlusXid xid11 = createJdbcXid(transaction);
            xaResource1PreparedXids.add(xid11);
            // xid11 - only prepared, not commited or rolledback
            xaResource1NoDecisionXids.add(xid11);
            // xaResource1DanglingXids.put(xid11, true);
            XAPlusXid xid12 = createJdbcXid(transaction);
            xaResource1PreparedXids.add(xid12);
            xaResource1DanglingXids.put(xid12, true);
            XAPlusXid xid13 = createJdbcXid(transaction);
            xaResource1PreparedXids.add(xid13);
            xaResource1DanglingXids.put(xid13, false);

            xaResource2PreparedXids = new HashSet<>();
            xaResource2DanglingXids = new HashMap<>();
            xaResource2NoDecisionXids = new HashSet<>();
            XAPlusXid xid21 = createJdbcXid(transaction);
            xaResource2PreparedXids.add(xid21);
            xaResource2DanglingXids.put(xid21, true);
            XAPlusXid xid22 = createJdbcXid(transaction);
            xaResource2PreparedXids.add(xid22);
            // xid22 - only prepared, not commited or rolledback
            xaResource2NoDecisionXids.add(xid22);
            // xaResource2DanglingXids.put(xid22, true);
            XAPlusXid xid23 = createJdbcXid(transaction);
            xaResource2PreparedXids.add(xid23);
            xaResource2DanglingXids.put(xid23, false);

            xaResource3PreparedXids = new HashSet<>();
            xaResource3DanglingXids = new HashMap<>();
            xaResource3NoDecisionXids = new HashSet<>();
            XAPlusXid xid31 = createJdbcXid(transaction);
            xaResource3PreparedXids.add(xid31);
            xaResource3DanglingXids.put(xid31, true);
            XAPlusXid xid32 = createJdbcXid(transaction);
            xaResource3PreparedXids.add(xid32);
            xaResource3DanglingXids.put(xid32, true);
            XAPlusXid xid33 = createJdbcXid(transaction);
            xaResource3PreparedXids.add(xid33);
            // xid33 - only prepared, not commited or rolledback
            xaResource3NoDecisionXids.add(xid33);
            // xaResource3DanglingXids.put(xid33, true);
        }
    }

    class XAPlusBranches extends XABranches {

        final Map<XAPlusXid, Boolean> xaPlusResource2DanglingXids;
        final Map<XAPlusXid, Boolean> xaPlusResource3DanglingXids;

        XAPlusBranches(XAPlusTransaction transaction) {
            super(transaction);

            xaPlusResource2DanglingXids = new HashMap<>();
            xaPlusResource2DanglingXids.put(createXAPlusXid(transaction, XA_PLUS_RESOURCE_2), true);
            xaPlusResource2DanglingXids.put(createXAPlusXid(transaction, XA_PLUS_RESOURCE_2), false);

            xaPlusResource3DanglingXids = new HashMap<>();
            xaPlusResource3DanglingXids.put(createXAPlusXid(transaction, XA_PLUS_RESOURCE_3), true);
            xaPlusResource3DanglingXids.put(createXAPlusXid(transaction, XA_PLUS_RESOURCE_3), false);
        }
    }

    class TestXAResources {
        Map<String, javax.sql.XAConnection> jdbcConnections;
        Map<String, javax.jms.XAJMSContext> jmsContexts;
        Map<String, XAResource> xaResources;

        TestXAResources() {
            jdbcConnections = new HashMap<>();
            jdbcConnections.put(XA_RESOURCE_1, new XAConnectionStub());
            jdbcConnections.put(XA_RESOURCE_2, new XAConnectionStub());
            jdbcConnections.put(XA_RESOURCE_3, new XAConnectionStub());

            jmsContexts = new HashMap<>();

            xaResources = new HashMap<>();
            xaResources.put(XA_RESOURCE_1, new XAResourceStub());
            xaResources.put(XA_RESOURCE_2, new XAResourceStub());
            xaResources.put(XA_RESOURCE_3, new XAResourceStub());
        }

        Map<String, XAConnection> getJdbcConnections() {
            return jdbcConnections;
        }

        Map<String, javax.jms.XAJMSContext> getJmsContexts() {
            return jmsContexts;
        }

        Map<String, XAResource> getXaResources() {
            return xaResources;
        }
    }
}
