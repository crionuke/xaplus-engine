package org.xaplus.engine;

import org.junit.Assert;
import org.xaplus.engine.stubs.XADataSourceStub;
import org.xaplus.engine.stubs.XAPlusFactoryStub;
import org.xaplus.engine.stubs.XAResourceStub;

public class XAPlusServiceTest extends Assert {
    protected final int QUEUE_SIZE = 128;
    protected final int DEFAULT_TIMEOUT_S = 10;
    protected final int POLL_TIMIOUT_MS = 2000;

    protected final String SERVER_ID = "server-stub";

    protected final String XA_RESOURCE_1 = "db1";
    protected final String XA_RESOURCE_2 = "db2";
    protected final String XA_RESOURCE_3 = "db3";

    protected final String XA_PLUS_RESOURCE_1 = "service1";
    protected final String XA_PLUS_RESOURCE_2 = "service2";

    protected XAPlusProperties properties;
    protected XAPlusResources resources;
    protected XAPlusUidGenerator uidGenerator;
    protected XAPlusThreadOfControl threadOfControl;
    protected XAPlusEngine engine;
    protected XAPlusThreadPool threadPool;
    protected XAPlusDispatcher dispatcher;

    protected void createXAPlusComponents() {
        createXAPlusComponents(DEFAULT_TIMEOUT_S);
    }

    protected void createXAPlusComponents(int defaultTimeoutInSeconds) {
        properties = new XAPlusProperties();
        properties.setServerId(SERVER_ID);
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
        return new XAPlusXid(uidGenerator.generateUid(properties.getServerId()),
                uidGenerator.generateUid(properties.getServerId()));
    }

    protected XAPlusXid createBranchXid(XAPlusTransaction transaction) {
        return uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), properties.getServerId());
    }

    protected XAPlusTransaction createTestSuperiorTransaction() {
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid bxid1 = createBranchXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAResourceStub());
        XAPlusXid bxid2 = createBranchXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAResourceStub());
        XAPlusXid bxid3 = createBranchXid(transaction);
        transaction.enlist(bxid3, XA_RESOURCE_3, new XAResourceStub());
        return transaction;
    }

    protected XAPlusTransaction createSuperiorTransaction() {
        return createSuperiorTransaction(properties.getDefaultTimeoutInSeconds());
    }

    protected XAPlusTransaction createSuperiorTransaction(int timeoutInSeconds) {
        XAPlusXid xid = generateSuperiorXid();
        XAPlusTransaction transaction = new XAPlusTransaction(xid, timeoutInSeconds, properties.getServerId());
        return transaction;
    }

    protected XAPlusTransaction createSubordinateTransaction() {
        return createSubordinateTransaction(properties.getDefaultTimeoutInSeconds());
    }

    protected XAPlusTransaction createSubordinateTransaction(int timeoutInSeconds) {
        XAPlusXid xid = new XAPlusXid(uidGenerator.generateUid("remote-server"),
                uidGenerator.generateUid("remote-server"));
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
