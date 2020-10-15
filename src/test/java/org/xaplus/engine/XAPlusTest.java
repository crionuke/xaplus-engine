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

    protected final String SERVER_ID_DEFAULT = "server1-stub";
    protected final String SERVER_ID_1 = SERVER_ID_DEFAULT;
    protected final String SERVER_ID_2 = "server2-stub";
    protected final String SERVER_ID_3 = "server3-stub";

    protected final String XA_RESOURCE_1 = "db1-stub";
    protected final String XA_RESOURCE_2 = "db2-stub";
    protected final String XA_RESOURCE_3 = "db3-stub";

    protected final String XA_PLUS_RESOURCE_1 = "service1-stub";
    protected final String XA_PLUS_RESOURCE_2 = "service2-stub";

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

    protected XAPlusXid generateSuperiorXid() {
        String serverId = properties.getServerId();
        return new XAPlusXid(uidGenerator.generateUid(serverId),
                uidGenerator.generateUid(serverId));
    }

    protected XAPlusTransaction createSuperiorTransaction() {
        return createSuperiorTransaction(properties.getDefaultTimeoutInSeconds());
    }

    protected XAPlusTransaction createSuperiorTransaction(int timeoutInSeconds) {
        String serverId = properties.getServerId();
        XAPlusXid xid = new XAPlusXid(uidGenerator.generateUid(serverId), uidGenerator.generateUid(serverId));
        XAPlusTransaction transaction = new XAPlusTransaction(xid, timeoutInSeconds, properties.getServerId());
        return transaction;
    }

    protected XAPlusXid enlistJdbc(XAPlusTransaction transaction) {
        return uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), properties.getServerId());
    }

    protected XAPlusXid enlistXAPlus(XAPlusTransaction transaction, String serverId) {
        return uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), serverId);
    }

    protected XAPlusXid createBranchXid(XAPlusTransaction transaction) {
        return uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), properties.getServerId());
    }

    protected XAPlusTransaction createTestSuperiorTransaction() {
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid bxid1 = enlistJdbc(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAResourceStub());
        XAPlusXid bxid2 = enlistJdbc(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAResourceStub());
        XAPlusXid bxid3 = enlistJdbc(transaction);
        transaction.enlist(bxid3, XA_RESOURCE_3, new XAResourceStub());
        XAPlusXid bxid4 = enlistXAPlus(transaction, XA_PLUS_RESOURCE_1);
        transaction.enlist(bxid4, XA_PLUS_RESOURCE_1, new XAPlusResourceStub());
        XAPlusXid bxid5 = enlistXAPlus(transaction, XA_PLUS_RESOURCE_2);
        transaction.enlist(bxid5, XA_PLUS_RESOURCE_2, new XAPlusResourceStub());
        return transaction;
    }

    protected XAPlusTransaction createSubordinateTransaction(String uniqueName) {
        return createSubordinateTransaction(uniqueName, properties.getDefaultTimeoutInSeconds());
    }

    protected XAPlusTransaction createSubordinateTransaction(String uniqueName, int timeoutInSeconds) {
        XAPlusXid xid = new XAPlusXid(uidGenerator.generateUid(uniqueName),
                uidGenerator.generateUid(uniqueName));
        XAPlusTransaction transaction = new XAPlusTransaction(xid, timeoutInSeconds, properties.getServerId());
        return transaction;
    }

    private void initResources(XAPlusResources resources) {
        resources.register(new XADataSourceStub(), XA_RESOURCE_1);
        resources.register(new XADataSourceStub(), XA_RESOURCE_2);
        resources.register(new XADataSourceStub(), XA_RESOURCE_3);
        resources.register(new XAPlusFactoryStub(), XA_PLUS_RESOURCE_1);
        resources.register(new XAPlusFactoryStub(), XA_PLUS_RESOURCE_2);
    }
}
