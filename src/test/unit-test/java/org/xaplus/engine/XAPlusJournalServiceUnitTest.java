package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.*;
import org.xaplus.engine.stubs.XAConnectionStub;
import org.xaplus.engine.stubs.XADataSourceStub;

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
    public void testLogCommitTransactionDecisionSuccessfully() throws InterruptedException, SQLException, XAException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
        XAPlusCommitTransactionDecisionLoggedEvent event =
                consumerStub.commitTransactionDecisionLoggedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogCommitTransactionDecisionFailed() throws InterruptedException, SQLException, XAException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Mockito.doThrow(new SQLException("log_exception")).when(tlogMock)
                .logCommitDecision(transaction.getXid().getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
        XAPlusLogCommitTransactionDecisionFailedEvent event =
                consumerStub.commitTransactionDecisionFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogRollbackTransactionDecisionSuccessfully() throws InterruptedException, SQLException,
            XAException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        XAPlusRollbackTransactionDecisionLoggedEvent event =
                consumerStub.rollbackTransactionDecisionLoggedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogRollbackTransactionDecisionFailed() throws InterruptedException, SQLException, XAException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Mockito.doThrow(new SQLException("log_exception")).when(tlogMock)
                .logRollbackDecision(transaction.getXid().getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        XAPlusLogRollbackTransactionDecisionFailedEvent event =
                consumerStub.rollbackTransactionDecisionFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testFindRecoveredXidStatusRequestEventSuccessfully() throws InterruptedException, SQLException, XAException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        XAPlusRecoveredResource recoveredResource =
                new XAPlusRecoveredResource(XA_RESOURCE_1, properties.getServerId(), System.currentTimeMillis(), new XAConnectionStub());
        // Test 1
        XAPlusXid bxid1 = createJdbcXid(transaction);
        Mockito.doReturn(true).when(tlogMock)
                .findTransactionStatus(bxid1.getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusFindRecoveredXidStatusRequestEvent(bxid1, recoveredResource));
        XAPlusRecoveredXidStatusFoundEvent event1 =
                consumerStub.recoveredXidStatusFoundEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertTrue(event1.getStatus());
        assertEquals(recoveredResource.getUniqueName(), event1.getRecoveredResource().getUniqueName());
        // Test 2
        XAPlusXid bxid2 = createJdbcXid(transaction);
        Mockito.doReturn(false).when(tlogMock)
                .findTransactionStatus(bxid2.getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusFindRecoveredXidStatusRequestEvent(bxid2, recoveredResource));
        XAPlusRecoveredXidStatusFoundEvent event2 =
                consumerStub.recoveredXidStatusFoundEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertFalse(event2.getStatus());
        assertEquals(recoveredResource.getUniqueName(), event2.getRecoveredResource().getUniqueName());
    }

    @Test
    public void testFindRecoveredXidStatusRequestEventFailed() throws InterruptedException, SQLException, XAException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        XAPlusRecoveredResource recoveredResource = new XAPlusRecoveredResource(XA_RESOURCE_1, properties.getServerId(),
                System.currentTimeMillis(), new XAConnectionStub());
        XAPlusXid xid = transaction.getXid();
        Mockito.doThrow(new SQLException("find_exception")).when(tlogMock)
                .findTransactionStatus(xid.getGlobalTransactionIdUid());
        dispatcher.dispatch(new XAPlusFindRecoveredXidStatusRequestEvent(xid, recoveredResource));
        XAPlusFindRecoveredXidStatusFailedEvent event =
                consumerStub.findRecoveredXidStatusFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(xid, event.getXid());
        assertEquals(recoveredResource.getUniqueName(), event.getRecoveredResource().getUniqueName());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusCommitTransactionDecisionLoggedEvent.Handler,
            XAPlusLogCommitTransactionDecisionFailedEvent.Handler,
            XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
            XAPlusLogRollbackTransactionDecisionFailedEvent.Handler,
            XAPlusRecoveredXidStatusFoundEvent.Handler,
            XAPlusFindRecoveredXidStatusFailedEvent.Handler {

        BlockingQueue<XAPlusCommitTransactionDecisionLoggedEvent> commitTransactionDecisionLoggedEvents;
        BlockingQueue<XAPlusLogCommitTransactionDecisionFailedEvent> commitTransactionDecisionFailedEvents;
        BlockingQueue<XAPlusRollbackTransactionDecisionLoggedEvent> rollbackTransactionDecisionLoggedEvents;
        BlockingQueue<XAPlusLogRollbackTransactionDecisionFailedEvent> rollbackTransactionDecisionFailedEvents;
        BlockingQueue<XAPlusRecoveredXidStatusFoundEvent> recoveredXidStatusFoundEvents;
        BlockingQueue<XAPlusFindRecoveredXidStatusFailedEvent> findRecoveredXidStatusFailedEvents;

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
            commitTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            commitTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            recoveredXidStatusFoundEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            findRecoveredXidStatusFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
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

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveredXidStatusFoundEvent.class);
            dispatcher.subscribe(this, XAPlusFindRecoveredXidStatusFailedEvent.class);
        }
    }
}
