package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.*;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateRetryRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRetryCommitOrderRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRetryRollbackOrderRequestEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;
import org.xaplus.engine.stubs.XAConnectionStub;

import javax.transaction.xa.XAException;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusJournalServiceUnitTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusJournalServiceUnitTest.class);

    private XAPlusTLog tlogMock;
    private XAPlusJournalService xaPlusJournalService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        tlogMock = Mockito.mock(XAPlusTLog.class);
        xaPlusJournalService = new XAPlusJournalService(properties, threadPool, dispatcher, resources, tlogMock);
        xaPlusJournalService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        consumerStub.finish();
        xaPlusJournalService.finish();
    }

    @Test
    public void testLogCommitTransactionDecisionSuccessfully() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
        XAPlusCommitTransactionDecisionLoggedEvent event =
                consumerStub.commitTransactionDecisionLoggedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogCommitTransactionDecisionFailed() throws InterruptedException, SQLException, XAException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        Mockito.doThrow(new SQLException("log_exception")).when(tlogMock)
                .logCommitDecision(transaction.getXid().getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
        XAPlusLogCommitTransactionDecisionFailedEvent event =
                consumerStub.commitTransactionDecisionFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogRollbackTransactionDecisionSuccessfully() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        XAPlusRollbackTransactionDecisionLoggedEvent event =
                consumerStub.rollbackTransactionDecisionLoggedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogRollbackTransactionDecisionFailed() throws InterruptedException, SQLException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        Mockito.doThrow(new SQLException("log_exception")).when(tlogMock)
                .logRollbackDecision(transaction.getXid().getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        XAPlusLogRollbackTransactionDecisionFailedEvent event =
                consumerStub.rollbackTransactionDecisionFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testFindRecoveredXidStatusRequestEventSuccessfully() throws InterruptedException, SQLException {
        XAPlusRecoveredResource recoveredResource = new XAPlusRecoveredResource(XA_RESOURCE_1, properties.getServerId(),
                System.currentTimeMillis(), new XAConnectionStub());
        // Test 1
        XAPlusXid bxid1 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_1), XAPlusUid.generate(XA_PLUS_RESOURCE_1));
        Mockito.doReturn(true).when(tlogMock)
                .findTransactionStatus(bxid1.getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusFindRecoveredXidStatusRequestEvent(bxid1, recoveredResource));
        XAPlusRecoveredXidStatusFoundEvent event1 =
                consumerStub.recoveredXidStatusFoundEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertTrue(event1.getStatus());
        assertEquals(recoveredResource.getUniqueName(), event1.getRecoveredResource().getUniqueName());
        // Test 2
        XAPlusXid bxid2 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_1), XAPlusUid.generate(XA_PLUS_RESOURCE_1));
        Mockito.doReturn(false).when(tlogMock)
                .findTransactionStatus(bxid2.getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusFindRecoveredXidStatusRequestEvent(bxid2, recoveredResource));
        XAPlusRecoveredXidStatusFoundEvent event2 =
                consumerStub.recoveredXidStatusFoundEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertFalse(event2.getStatus());
        assertEquals(recoveredResource.getUniqueName(), event2.getRecoveredResource().getUniqueName());
    }

    @Test
    public void testFindRecoveredXidStatusRequestEventFailed() throws InterruptedException, SQLException {
        XAPlusXid xid = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_1), XAPlusUid.generate(XA_PLUS_RESOURCE_1));
        XAPlusRecoveredResource recoveredResource = new XAPlusRecoveredResource(XA_RESOURCE_1, properties.getServerId(),
                System.currentTimeMillis(), new XAConnectionStub());
        Mockito.doThrow(new SQLException("find_exception")).when(tlogMock)
                .findTransactionStatus(xid.getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusFindRecoveredXidStatusRequestEvent(xid, recoveredResource));
        XAPlusFindRecoveredXidStatusFailedEvent event =
                consumerStub.findRecoveredXidStatusFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(xid, event.getXid());
        assertEquals(recoveredResource.getUniqueName(), event.getRecoveredResource().getUniqueName());
    }

    @Test
    public void testRemoteSubordinateRetryRequestEvent()
            throws InterruptedException, SQLException, XAPlusSystemException {
        // Test 1
        XAPlusXid bxid1 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_1), XAPlusUid.generate(XA_PLUS_RESOURCE_2));
        Mockito.doReturn(true).when(tlogMock).findTransactionStatus(bxid1.getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusRemoteSubordinateRetryRequestEvent(bxid1));
        XAPlusRetryCommitOrderRequestEvent event1 =
                consumerStub.retryCommitOrderRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(bxid1, event1.getXid());
        assertEquals(resources.getXAPlusResource(XA_PLUS_RESOURCE_2), event1.getResource());
        // Test 2
        XAPlusXid bxid2 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_1), XAPlusUid.generate(XA_PLUS_RESOURCE_3));
        Mockito.doReturn(false).when(tlogMock).findTransactionStatus(bxid2.getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusRemoteSubordinateRetryRequestEvent(bxid2));
        XAPlusRetryRollbackOrderRequestEvent event2 =
                consumerStub.retryRollbackOrderRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(bxid2, event2.getXid());
        assertEquals(resources.getXAPlusResource(XA_PLUS_RESOURCE_3), event2.getResource());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusCommitTransactionDecisionLoggedEvent.Handler,
            XAPlusLogCommitTransactionDecisionFailedEvent.Handler,
            XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
            XAPlusLogRollbackTransactionDecisionFailedEvent.Handler,
            XAPlusRecoveredXidStatusFoundEvent.Handler,
            XAPlusFindRecoveredXidStatusFailedEvent.Handler,
            XAPlusRetryCommitOrderRequestEvent.Handler,
            XAPlusRetryRollbackOrderRequestEvent.Handler {

        BlockingQueue<XAPlusCommitTransactionDecisionLoggedEvent> commitTransactionDecisionLoggedEvents;
        BlockingQueue<XAPlusLogCommitTransactionDecisionFailedEvent> commitTransactionDecisionFailedEvents;
        BlockingQueue<XAPlusRollbackTransactionDecisionLoggedEvent> rollbackTransactionDecisionLoggedEvents;
        BlockingQueue<XAPlusLogRollbackTransactionDecisionFailedEvent> rollbackTransactionDecisionFailedEvents;
        BlockingQueue<XAPlusRecoveredXidStatusFoundEvent> recoveredXidStatusFoundEvents;
        BlockingQueue<XAPlusFindRecoveredXidStatusFailedEvent> findRecoveredXidStatusFailedEvents;
        BlockingQueue<XAPlusRetryCommitOrderRequestEvent> retryCommitOrderRequestEvents;
        BlockingQueue<XAPlusRetryRollbackOrderRequestEvent> retryRollbackOrderRequestEvents;

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
            commitTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            commitTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            recoveredXidStatusFoundEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            findRecoveredXidStatusFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            retryCommitOrderRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            retryRollbackOrderRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleCommitTransactionDecisionLogged(XAPlusCommitTransactionDecisionLoggedEvent event)
                throws InterruptedException {
            commitTransactionDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleLogCommitTransactionDecisionFailed(XAPlusLogCommitTransactionDecisionFailedEvent event)
                throws InterruptedException {
            commitTransactionDecisionFailedEvents.put(event);
        }

        @Override
        public void handleRollbackTransactionDecisionLogged(XAPlusRollbackTransactionDecisionLoggedEvent event)
                throws InterruptedException {
            rollbackTransactionDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleLogRollbackTransactionDecisionFailed(XAPlusLogRollbackTransactionDecisionFailedEvent event) throws InterruptedException {
            rollbackTransactionDecisionFailedEvents.put(event);
        }

        @Override
        public void handleRecoveredXidStatusFound(XAPlusRecoveredXidStatusFoundEvent event) throws InterruptedException {
            recoveredXidStatusFoundEvents.put(event);
        }

        @Override
        public void handleFindRecoveredXidStatusFailed(XAPlusFindRecoveredXidStatusFailedEvent event) throws InterruptedException {
            findRecoveredXidStatusFailedEvents.put(event);
        }

        @Override
        public void handleRetryCommitOrderRequest(XAPlusRetryCommitOrderRequestEvent event) throws InterruptedException {
            retryCommitOrderRequestEvents.put(event);
        }

        @Override
        public void handleRetryRollbackOrderRequest(XAPlusRetryRollbackOrderRequestEvent event) throws InterruptedException {
            retryRollbackOrderRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveredXidStatusFoundEvent.class);
            dispatcher.subscribe(this, XAPlusFindRecoveredXidStatusFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRetryCommitOrderRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRetryRollbackOrderRequestEvent.class);
        }
    }
}
