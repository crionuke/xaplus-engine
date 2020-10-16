package org.xaplus.engine;

import org.junit.Assert;
import org.xaplus.engine.stubs.XADataSourceStub;
import org.xaplus.engine.stubs.XAPlusFactoryStub;
import org.xaplus.engine.stubs.XAPlusResourceStub;
import org.xaplus.engine.stubs.XAResourceStub;

public class XAPlusTest extends Assert {
    protected final int QUEUE_SIZE = 128;
    protected final int DEFAULT_TIMEOUT_S = 10;
    protected final int POLL_TIMIOUT_MS = 2000;
    protected final int VERIFY_MS = 1000;

    protected final String XA_RESOURCE_1 = "db1-stub";
    protected final String XA_RESOURCE_2 = "db2-stub";
    protected final String XA_RESOURCE_3 = "db3-stub";

    protected final String XA_PLUS_RESOURCE_1 = "server1-stub";
    protected final String XA_PLUS_RESOURCE_2 = "server2-stub";
    protected final String XA_PLUS_RESOURCE_3 = "server3-stub";

    protected XAPlusProperties properties;
    protected XAPlusResources resources;
    protected XAPlusUidGenerator uidGenerator;
    protected XAPlusThreadOfControl threadOfControl;
    protected XAPlusEngine engine;
    protected XAPlusThreadPool threadPool;
    protected XAPlusDispatcher dispatcher;

    protected void createXAPlusComponents(String serverId) {
        createXAPlusComponents(serverId, DEFAULT_TIMEOUT_S);
    }

    protected void createXAPlusComponents(String serverId, int defaultTimeoutInSeconds) {
        properties = new XAPlusProperties();
        properties.setServerId(serverId);
        properties.setQueueSize(QUEUE_SIZE);
        properties.setDefaultTimeoutInSeconds(defaultTimeoutInSeconds);
        resources = new XAPlusResources();
        initResources(resources);
        uidGenerator = new XAPlusUidGenerator();
        threadOfControl = new XAPlusThreadOfControl();
        engine = new XAPlusEngine(properties, dispatcher, resources, uidGenerator, threadOfControl);
        threadPool = new XAPlusThreadPool();
        dispatcher = new XAPlusDispatcher();
    }

    protected XAPlusTransaction createSuperiorTransaction() {
        return createSuperiorTransaction(properties.getDefaultTimeoutInSeconds());
    }

    protected XAPlusTransaction createSuperiorTransaction(int timeoutInSeconds) {
        return createSuperiorTransaction(XA_PLUS_RESOURCE_1, timeoutInSeconds);
    }

    protected XAPlusTransaction createSuperiorTransaction(String superiorServerId, int timeoutInSeconds) {
        XAPlusXid xid = new XAPlusXid(uidGenerator.generateUid(superiorServerId),
                uidGenerator.generateUid(superiorServerId));
        XAPlusTransaction transaction = new XAPlusTransaction(xid, timeoutInSeconds, superiorServerId);
        return transaction;
    }

    protected XAPlusTransaction createSubordinateTransaction(String subordinateServerId) {
        return createSubordinateTransaction(XA_PLUS_RESOURCE_1, subordinateServerId);
    }

    protected XAPlusTransaction createSubordinateTransaction(String superiorServerId, String subordinateServerId) {
        XAPlusXid xid = new XAPlusXid(uidGenerator.generateUid(superiorServerId),
                uidGenerator.generateUid(subordinateServerId));
        XAPlusTransaction transaction = new XAPlusTransaction(xid, properties.getDefaultTimeoutInSeconds(),
                subordinateServerId);
        return transaction;
    }

    protected XAPlusXid createJdbcXid(XAPlusTransaction transaction) {
        return uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), properties.getServerId());
    }

    protected XAPlusXid createXAPlusXid(XAPlusTransaction transaction, String serverId) {
        return uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), serverId);
    }

    protected XAPlusTransaction createTestSuperiorTransaction() {
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid bxid1 = createJdbcXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAResourceStub());
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAResourceStub());
        XAPlusXid bxid3 = createJdbcXid(transaction);
        transaction.enlist(bxid3, XA_RESOURCE_3, new XAResourceStub());
        XAPlusXid bxid4 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_2);
        transaction.enlist(bxid4, XA_PLUS_RESOURCE_2, new XAPlusResourceStub());
        XAPlusXid bxid5 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_3);
        transaction.enlist(bxid5, XA_PLUS_RESOURCE_3, new XAPlusResourceStub());
        return transaction;
    }

    private void initResources(XAPlusResources resources) {
        resources.register(new XADataSourceStub(), XA_RESOURCE_1);
        resources.register(new XADataSourceStub(), XA_RESOURCE_2);
        resources.register(new XADataSourceStub(), XA_RESOURCE_3);
        resources.register(new XAPlusFactoryStub(), XA_PLUS_RESOURCE_1);
        resources.register(new XAPlusFactoryStub(), XA_PLUS_RESOURCE_2);
        resources.register(new XAPlusFactoryStub(), XA_PLUS_RESOURCE_3);
    }
}
