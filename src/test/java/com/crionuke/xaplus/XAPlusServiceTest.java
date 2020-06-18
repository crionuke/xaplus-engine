package com.crionuke.xaplus;

import org.junit.Assert;

public class XAPlusServiceTest extends Assert {
    protected final int QUEUE_SIZE = 128;
    protected final int DEFAULT_TIMEOUT_S = 10;
    protected final int POLL_TIMIOUT_MS = 2000;

    protected XAPlusProperties properties;
    protected XAPlusUidGenerator uidGenerator;
    protected XAPlusThreadPool threadPool;
    protected XAPlusDispatcher dispatcher;

    protected void createXAPlusComponents() {
        createXAPlusComponents(DEFAULT_TIMEOUT_S);
    }

    protected void createXAPlusComponents(int defaultTimeoutInSeconds) {
        properties = new XAPlusProperties();
        properties.setServerId("test-server");
        properties.setQueueSize(QUEUE_SIZE);
        properties.setDefaultTimeoutInSeconds(defaultTimeoutInSeconds);
        uidGenerator = new XAPlusUidGenerator(properties);
        threadPool = new XAPlusThreadPool();
        dispatcher = new XAPlusDispatcher();
    }

    protected XAPlusTransaction createTransaction() {
        return createTransaction(properties.getDefaultTimeoutInSeconds());
    }

    protected XAPlusTransaction createTransaction(int timeoutInSeconds) {
        XAPlusXid xid = new XAPlusXid(uidGenerator.generateUid(properties.getServerId()),
                uidGenerator.generateUid(properties.getServerId()));
        XAPlusTransaction transaction = new XAPlusTransaction(xid, timeoutInSeconds, properties.getServerId());
        return transaction;
    }
}
