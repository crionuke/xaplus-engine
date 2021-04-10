package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.*;

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

    private class ConsumerStub extends Bolt implements
            XAPlusCommitTransactionDecisionLoggedEvent.Handler,
            XAPlusLogCommitTransactionDecisionFailedEvent.Handler,
            XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
            XAPlusLogRollbackTransactionDecisionFailedEvent.Handler,
            XAPlusCommitRecoveredXidDecisionLoggedEvent.Handler,
            XAPlusRollbackRecoveredXidDecisionLoggedEvent.Handler {

        BlockingQueue<XAPlusCommitTransactionDecisionLoggedEvent> commitTransactionDecisionLoggedEvents;
        BlockingQueue<XAPlusLogCommitTransactionDecisionFailedEvent> commitTransactionDecisionFailedEvents;
        BlockingQueue<XAPlusRollbackTransactionDecisionLoggedEvent> rollbackTransactionDecisionLoggedEvents;
        BlockingQueue<XAPlusLogRollbackTransactionDecisionFailedEvent> rollbackTransactionDecisionFailedEvents;
        BlockingQueue<XAPlusCommitRecoveredXidDecisionLoggedEvent> commitRecoveredXidDecisionLoggedEvents;
        BlockingQueue<XAPlusRollbackRecoveredXidDecisionLoggedEvent> rollbackRecoveredXidDecisionLoggedEvents;

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
            commitTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            commitTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            commitRecoveredXidDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackRecoveredXidDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
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

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusCommitRecoveredXidDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRecoveredXidDecisionLoggedEvent.class);
        }
    }
}
