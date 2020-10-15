package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusServiceIntegrationTest extends XAPlusIntegrationTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusServiceIntegrationTest.class);

    XAPlusService xaPlusService;

    BlockingQueue<XAPlusBranchPreparedEvent> branchPreparedEvents;
    BlockingQueue<XAPlusPrepareBranchFailedEvent> prepareBranchFailedEvents;
    BlockingQueue<XAPlusBranchCommittedEvent> branchCommittedEvents;
    BlockingQueue<XAPlusCommitBranchFailedEvent> commitBranchFailedEvents;
    BlockingQueue<XAPlusBranchRolledBackEvent> branchRolledBackEvents;
    BlockingQueue<XAPlusRollbackBranchFailedEvent> rollbackBranchFailedEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(SERVER_ID_DEFAULT);
        createXADataSource();

        xaPlusService = new XAPlusService(properties, threadPool, dispatcher);
        xaPlusService.postConstruct();

        branchPreparedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        prepareBranchFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        branchCommittedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitBranchFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        branchRolledBackEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackBranchFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

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
        try (OneBranchTransaction transaction = new OneBranchTransaction(properties.getServerId())) {
            // Prepare
            transaction.start();
            transaction.insert();
            // Use
            dispatcher.dispatch(new XAPlusPrepareBranchRequestEvent(transaction.getXid(),
                    transaction.getBranchXid(), transaction.getXaResource()));
            XAPlusBranchPreparedEvent event = branchPreparedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            // Assert
            assertNotNull(event);
            assertEquals(transaction.getXid(), event.getXid());
            assertEquals(transaction.getBranchXid(), event.getBranchXid());
        }
    }

    @Test
    public void testPrepareBranchRequestFailed() throws Exception {
        try (OneBranchTransaction transaction = new OneBranchTransaction(properties.getServerId())) {
            // Prepare - no branch start for prepare failure simulation
            transaction.insert();
            // Use
            dispatcher.dispatch(new XAPlusPrepareBranchRequestEvent(transaction.getXid(),
                    transaction.getBranchXid(), transaction.getXaResource()));
            XAPlusPrepareBranchFailedEvent event =
                    prepareBranchFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            // Assert
            assertNotNull(event);
            assertEquals(transaction.getXid(), event.getXid());
            assertEquals(transaction.getBranchXid(), event.getBranchXid());
        }
    }

    @Test
    public void testCommitBranchRequestSuccessfully() throws Exception {
        try (OneBranchTransaction transaction = new OneBranchTransaction(properties.getServerId())) {
            // Prepare
            transaction.start();
            transaction.insert();
            transaction.end();
            transaction.prepare();
            // Use
            dispatcher.dispatch(new XAPlusCommitBranchRequestEvent(transaction.getXid(),
                    transaction.getBranchXid(), transaction.getXaResource()));
            XAPlusBranchCommittedEvent event = branchCommittedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            // Assert
            assertNotNull(event);
            assertEquals(transaction.getXid(), event.getXid());
            assertEquals(transaction.getBranchXid(), event.getBranchXid());
        }
    }

    @Test
    public void testCommitBranchRequestFailed() throws Exception {
        try (OneBranchTransaction transaction = new OneBranchTransaction(properties.getServerId())) {
            // Prepare - no prepare phase to commit failure simulation
            transaction.start();
            transaction.insert();
            transaction.end();
            // Use
            dispatcher.dispatch(new XAPlusCommitBranchRequestEvent(transaction.getXid(),
                    transaction.getBranchXid(), transaction.getXaResource()));
            XAPlusCommitBranchFailedEvent event = commitBranchFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            // Assert
            assertNotNull(event);
            assertEquals(transaction.getXid(), event.getXid());
            assertEquals(transaction.getBranchXid(), event.getBranchXid());
        }
    }

    @Test
    public void testRollbackBranchRequestSuccessfully() throws Exception {
        try (OneBranchTransaction transaction = new OneBranchTransaction(properties.getServerId())) {
            // Prepare
            transaction.start();
            transaction.insert();
            // Use
            dispatcher.dispatch(new XAPlusRollbackBranchRequestEvent(transaction.getXid(),
                    transaction.getBranchXid(), transaction.getXaResource()));
            XAPlusBranchRolledBackEvent event = branchRolledBackEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            // Assert
            assertNotNull(event);
            assertEquals(transaction.getXid(), event.getXid());
            assertEquals(transaction.getBranchXid(), event.getBranchXid());
        }
    }

    @Test
    public void testRollbackBranchRequestFailed() throws Exception {
        try (OneBranchTransaction transaction = new OneBranchTransaction(properties.getServerId())) {
            // Prepare - no start to rollback failure simulation
            transaction.insert();
            // Use
            dispatcher.dispatch(new XAPlusRollbackBranchRequestEvent(transaction.getXid(),
                    transaction.getBranchXid(), transaction.getXaResource()));
            XAPlusRollbackBranchFailedEvent event =
                    rollbackBranchFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            // Assert
            assertNotNull(event);
            assertEquals(transaction.getXid(), event.getXid());
            assertEquals(transaction.getBranchXid(), event.getBranchXid());
        }
    }

    private class ConsumerStub extends Bolt implements
            XAPlusBranchPreparedEvent.Handler,
            XAPlusPrepareBranchFailedEvent.Handler,
            XAPlusBranchCommittedEvent.Handler,
            XAPlusCommitBranchFailedEvent.Handler,
            XAPlusBranchRolledBackEvent.Handler,
            XAPlusRollbackBranchFailedEvent.Handler {

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
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
