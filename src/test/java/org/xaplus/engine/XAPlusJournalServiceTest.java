package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;
import org.xaplus.engine.events.journal.*;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusJournalServiceTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusJournalServiceTest.class);

    XAPlusTLog tlogMock;
    XAPlusJournalService xaPlusJournalService;

    BlockingQueue<XAPlusCommitTransactionDecisionLoggedEvent> commitTransactionDecisionLoggedEvents;
    BlockingQueue<XAPlusLogCommitTransactionDecisionFailedEvent> commitTransactionDecisionFailedEvents;
    BlockingQueue<XAPlusRollbackTransactionDecisionLoggedEvent> rollbackTransactionDecisionLoggedEvents;
    BlockingQueue<XAPlusLogRollbackTransactionDecisionFailedEvent> rollbackTransactionDecisionFailedEvents;
    BlockingQueue<XAPlusCommitRecoveredXidDecisionLoggedEvent> commitRecoveredXidDecisionLoggedEvents;
    BlockingQueue<XAPlusRollbackRecoveredXidDecisionLoggedEvent> rollbackRecoveredXidDecisionLoggedEvents;
    BlockingQueue<XAPlusDanglingTransactionsFoundEvent> danglingTransactionsFoundEvents;
    BlockingQueue<XAPlusFindDanglingTransactionsFailedEvent> findDanglingTransactionsFailedEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);

        tlogMock = Mockito.mock(XAPlusTLog.class);
        xaPlusJournalService = new XAPlusJournalService(properties, threadPool, dispatcher, engine, tlogMock);
        xaPlusJournalService.postConstruct();

        commitTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitRecoveredXidDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRecoveredXidDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        danglingTransactionsFoundEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        findDanglingTransactionsFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

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
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
        XAPlusCommitTransactionDecisionLoggedEvent event = commitTransactionDecisionLoggedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogCommitTransactionDecisionFailed() throws InterruptedException, SQLException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Mockito.doThrow(new SQLException("log_exception")).when(tlogMock)
                .logCommitTransactionDecision(transaction);
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction));
        XAPlusLogCommitTransactionDecisionFailedEvent event = commitTransactionDecisionFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogRollbackTransactionDecisionSuccessfully() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        XAPlusRollbackTransactionDecisionLoggedEvent event = rollbackTransactionDecisionLoggedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogRollbackTransactionDecisionFailed() throws InterruptedException, SQLException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Mockito.doThrow(new SQLException("log_exception")).when(tlogMock)
                .logRollbackTransactionDecision(transaction);
        dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionEvent(transaction));
        XAPlusLogRollbackTransactionDecisionFailedEvent event = rollbackTransactionDecisionFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getTransaction().getXid());
    }

    @Test
    public void testLogCommitRecoveredXidDecisionSuccessfully() throws InterruptedException {
        String uniqueName = XA_RESOURCE_1;
        XAPlusTransaction transaction = createSuperiorTransaction();
        dispatcher.dispatch(new XAPlusLogCommitRecoveredXidDecisionEvent(transaction.getXid(), uniqueName));
        XAPlusCommitRecoveredXidDecisionLoggedEvent event = commitRecoveredXidDecisionLoggedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getXid());
        assertEquals(uniqueName, event.getUniqueName());
    }

    @Test
    public void testLogRollbackRecoveredXidDecisionSuccessfully() throws InterruptedException {
        String uniqueName = XA_RESOURCE_1;
        XAPlusTransaction transaction = createSuperiorTransaction();
        dispatcher.dispatch(new XAPlusLogRollbackRecoveredXidDecisionEvent(transaction.getXid(), uniqueName));
        XAPlusRollbackRecoveredXidDecisionLoggedEvent event = rollbackRecoveredXidDecisionLoggedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getXid());
        assertEquals(uniqueName, event.getUniqueName());
    }

    @Test
    public void testRecoveredXidCommitted() throws InterruptedException, SQLException {
        String uniqueName = XA_RESOURCE_1;
        XAPlusTransaction transaction = createSuperiorTransaction();
        dispatcher.dispatch(new XAPlusRecoveredXidCommittedEvent(transaction.getXid(), uniqueName));
        Mockito.verify(tlogMock, Mockito.timeout(VERIFY_MS)).logXidCommitted(transaction.getXid(), uniqueName);
    }

    @Test
    public void testRecoveredXidRolledBack() throws InterruptedException, SQLException {
        String uniqueName = XA_RESOURCE_1;
        XAPlusTransaction transaction = createSuperiorTransaction();
        dispatcher.dispatch(new XAPlusRecoveredXidRolledBackEvent(transaction.getXid(), uniqueName));
        Mockito.verify(tlogMock, Mockito.timeout(VERIFY_MS)).logXidRolledBack(transaction.getXid(), uniqueName);
    }

    @Test
    public void testDanglingTransactionCommitted() throws InterruptedException, SQLException {
        String uniqueName = XA_RESOURCE_1;
        XAPlusTransaction transaction = createSuperiorTransaction();
        dispatcher.dispatch(new XAPlusDanglingTransactionCommittedEvent(transaction.getXid(), uniqueName));
        Mockito.verify(tlogMock, Mockito.timeout(VERIFY_MS)).logXidCommitted(transaction.getXid(), uniqueName);
    }

    @Test
    public void testDanglingTransactionRolledBack() throws InterruptedException, SQLException {
        String uniqueName = XA_RESOURCE_1;
        XAPlusTransaction transaction = createSuperiorTransaction();
        dispatcher.dispatch(new XAPlusDanglingTransactionRolledBackEvent(transaction.getXid(), uniqueName));
        Mockito.verify(tlogMock, Mockito.timeout(VERIFY_MS)).logXidRolledBack(transaction.getXid(), uniqueName);
    }

    @Test
    public void test2pcDone() throws InterruptedException, SQLException {
        XAPlusTransaction transaction = createSuperiorTransaction();
        dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        Mockito.verify(tlogMock, Mockito.timeout(VERIFY_MS)).logTransactionCommitted(transaction);
    }

    @Test
    public void testFindDanglingTransactionsRequestSuccessfully() throws InterruptedException, SQLException {
        Map<String, Map<XAPlusXid, Boolean>> danglingTransactions = new HashMap<>();
        Mockito.when(tlogMock.findDanglingTransactions()).thenReturn(danglingTransactions);
        dispatcher.dispatch(new XAPlusFindDanglingTransactionsRequestEvent());
        XAPlusDanglingTransactionsFoundEvent event = danglingTransactionsFoundEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(danglingTransactions, event.getDanglingTransactions());
    }

    @Test
    public void testFindDanglingTransactionsRequestFailed() throws InterruptedException, SQLException {
        Mockito.doThrow(new SQLException("find_exception")).when(tlogMock)
                .findDanglingTransactions();
        dispatcher.dispatch(new XAPlusFindDanglingTransactionsRequestEvent());
        XAPlusFindDanglingTransactionsFailedEvent event = findDanglingTransactionsFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
    }

    private class ConsumerStub extends Bolt implements
            XAPlusCommitTransactionDecisionLoggedEvent.Handler,
            XAPlusLogCommitTransactionDecisionFailedEvent.Handler,
            XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
            XAPlusLogRollbackTransactionDecisionFailedEvent.Handler,
            XAPlusCommitRecoveredXidDecisionLoggedEvent.Handler,
            XAPlusRollbackRecoveredXidDecisionLoggedEvent.Handler,
            XAPlusDanglingTransactionsFoundEvent.Handler,
            XAPlusFindDanglingTransactionsFailedEvent.Handler {

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
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
        public void handleCommitRecoveredXidDecisionLogged(XAPlusCommitRecoveredXidDecisionLoggedEvent event)
                throws InterruptedException {
            commitRecoveredXidDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleRollbackRecoveredXidDecisionLogged(XAPlusRollbackRecoveredXidDecisionLoggedEvent event)
                throws InterruptedException {
            rollbackRecoveredXidDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleDanglingTransactionFound(XAPlusDanglingTransactionsFoundEvent event) throws InterruptedException {
            danglingTransactionsFoundEvents.put(event);
        }

        @Override
        public void handleFindDanglingTransactionsFailed(XAPlusFindDanglingTransactionsFailedEvent event) throws InterruptedException {
            findDanglingTransactionsFailedEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusCommitRecoveredXidDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRecoveredXidDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusDanglingTransactionsFoundEvent.class);
            dispatcher.subscribe(this, XAPlusFindDanglingTransactionsFailedEvent.class);
        }
    }
}
