package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.xa.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusServiceIntegrationTest extends XAPlusIntegrationTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusServiceIntegrationTest.class);

    private XAPlusService xaPlusService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        xaPlusService = new XAPlusService(properties, threadPool, dispatcher);
        xaPlusService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusService.finish();
        consumerStub.finish();
    }

    @Test
    public void testPrepareBranchRequestSuccessfully() throws Exception {
        XAPlusTestTransaction transaction = new XAPlusTestTransaction(createXADataSource(), properties.getServerId());
        transaction.start();
        transaction.insert();
        XAPlusXid xid = transaction.getXid();
        XAPlusXid branchXid = transaction.getBranchXid();
        dispatcher.dispatch(new XAPlusPrepareBranchRequestEvent(xid, branchXid, transaction.getXaResource()));
        XAPlusBranchPreparedEvent event = consumerStub
                .branchPreparedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(xid, event.getXid());
        assertEquals(branchXid, event.getBranchXid());
    }

    @Test
    public void testPrepareBranchRequestFailed() throws Exception {
        XAPlusTestTransaction transaction = new XAPlusTestTransaction(createXADataSource(), properties.getServerId());
        transaction.insert();
        XAPlusXid xid = transaction.getXid();
        XAPlusXid branchXid = transaction.getBranchXid();
        dispatcher.dispatch(new XAPlusPrepareBranchRequestEvent(xid, branchXid, transaction.getXaResource()));
        XAPlusPrepareBranchFailedEvent event = consumerStub
                .prepareBranchFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(xid, event.getXid());
        assertEquals(branchXid, event.getBranchXid());
    }

    @Test
    public void testCommitBranchRequestSuccessfully() throws Exception {
        XAPlusTestTransaction transaction = new XAPlusTestTransaction(createXADataSource(), properties.getServerId());
        transaction.start();
        transaction.insert();
        transaction.end();
        transaction.prepare();
        XAPlusXid xid = transaction.getXid();
        XAPlusXid branchXid = transaction.getBranchXid();
        dispatcher.dispatch(new XAPlusCommitBranchRequestEvent(xid, branchXid, transaction.getXaResource()));
        XAPlusBranchCommittedEvent event = consumerStub
                .branchCommittedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(xid, event.getXid());
        assertEquals(branchXid, event.getBranchXid());
    }

    @Test
    public void testCommitBranchRequestFailed() throws Exception {
        XAPlusTestTransaction transaction = new XAPlusTestTransaction(createXADataSource(), properties.getServerId());
        transaction.start();
        transaction.insert();
        transaction.end();
        XAPlusXid xid = transaction.getXid();
        XAPlusXid branchXid = transaction.getBranchXid();
        dispatcher.dispatch(new XAPlusCommitBranchRequestEvent(xid, branchXid, transaction.getXaResource()));
        XAPlusCommitBranchFailedEvent event = consumerStub
                .commitBranchFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(xid, event.getXid());
        assertEquals(branchXid, event.getBranchXid());
    }

    @Test
    public void testRollbackBranchRequestSuccessfully() throws Exception {
        XAPlusTestTransaction transaction = new XAPlusTestTransaction(createXADataSource(), properties.getServerId());
        transaction.start();
        transaction.insert();
        XAPlusXid xid = transaction.getXid();
        XAPlusXid branchXid = transaction.getBranchXid();
        dispatcher.dispatch(new XAPlusRollbackBranchRequestEvent(xid, branchXid, transaction.getXaResource()));
        XAPlusBranchRolledBackEvent event = consumerStub
                .branchRolledBackEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(xid, event.getXid());
        assertEquals(branchXid, event.getBranchXid());
    }

    @Test
    public void testRollbackBranchRequestFailed() throws Exception {
        XAPlusTestTransaction transaction = new XAPlusTestTransaction(createXADataSource(), properties.getServerId());
        transaction.insert();
        XAPlusXid xid = transaction.getXid();
        XAPlusXid branchXid = transaction.getBranchXid();
        dispatcher.dispatch(new XAPlusRollbackBranchRequestEvent(xid, branchXid, transaction.getXaResource()));
        XAPlusRollbackBranchFailedEvent event = consumerStub
                .rollbackBranchFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(xid, event.getXid());
        assertEquals(branchXid, event.getBranchXid());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusBranchPreparedEvent.Handler,
            XAPlusPrepareBranchFailedEvent.Handler,
            XAPlusBranchCommittedEvent.Handler,
            XAPlusCommitBranchFailedEvent.Handler,
            XAPlusBranchRolledBackEvent.Handler,
            XAPlusRollbackBranchFailedEvent.Handler {

        BlockingQueue<XAPlusBranchPreparedEvent> branchPreparedEvents;
        BlockingQueue<XAPlusPrepareBranchFailedEvent> prepareBranchFailedEvents;
        BlockingQueue<XAPlusBranchCommittedEvent> branchCommittedEvents;
        BlockingQueue<XAPlusCommitBranchFailedEvent> commitBranchFailedEvents;
        BlockingQueue<XAPlusBranchRolledBackEvent> branchRolledBackEvents;
        BlockingQueue<XAPlusRollbackBranchFailedEvent> rollbackBranchFailedEvents;

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);

            branchPreparedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            prepareBranchFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            branchCommittedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            commitBranchFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            branchRolledBackEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackBranchFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleBranchPrepared(XAPlusBranchPreparedEvent event) throws InterruptedException {
            branchPreparedEvents.put(event);
        }

        @Override
        public void handlePrepareBranchFailed(XAPlusPrepareBranchFailedEvent event) throws InterruptedException {
            prepareBranchFailedEvents.put(event);
        }

        @Override
        public void handleBranchCommitted(XAPlusBranchCommittedEvent event) throws InterruptedException {
            branchCommittedEvents.put(event);
        }

        @Override
        public void handleCommitBranchFailed(XAPlusCommitBranchFailedEvent event) throws InterruptedException {
            commitBranchFailedEvents.put(event);
        }

        @Override
        public void handleBranchRolledBack(XAPlusBranchRolledBackEvent event) throws InterruptedException {
            branchRolledBackEvents.put(event);
        }

        @Override
        public void handleRollbackBranchFailed(XAPlusRollbackBranchFailedEvent event) throws InterruptedException {
            rollbackBranchFailedEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusBranchPreparedEvent.class);
            dispatcher.subscribe(this, XAPlusPrepareBranchFailedEvent.class);
            dispatcher.subscribe(this, XAPlusBranchCommittedEvent.class);
            dispatcher.subscribe(this, XAPlusCommitBranchFailedEvent.class);
            dispatcher.subscribe(this, XAPlusBranchRolledBackEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackBranchFailedEvent.class);
        }
    }
}
