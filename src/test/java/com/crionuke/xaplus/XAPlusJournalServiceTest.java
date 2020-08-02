package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
public class XAPlusJournalServiceTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusJournalServiceTest.class);

    XAPlusTLog tlog;
    XAPlusJournalService xaPlusJournalService;

    BlockingQueue<XAPlusCommitTransactionDecisionLoggedEvent> commitTransactionDecisionLoggedEvents;
    BlockingQueue<XAPlusCommitTransactionDecisionFailedEvent> commitTransactionDecisionFailedEvents;
    BlockingQueue<XAPlusRollbackTransactionDecisionLoggedEvent> rollbackTransactionDecisionLoggedEvents;
    BlockingQueue<XAPlusCommitRecoveredXidDecisionLoggedEvent> commitRecoveredXidDecisionLoggedEvents;
    BlockingQueue<XAPlusRollbackRecoveredXidDecisionLoggedEvent> rollbackRecoveredXidDecisionLoggedEvents;
    BlockingQueue<XAPlusDanglingTransactionsFoundEvent> danglingTransactionsFoundEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents();

        tlog = Mockito.mock(XAPlusTLog.class);
        xaPlusJournalService = new XAPlusJournalService(properties, threadPool, dispatcher, engine, tlog);
        xaPlusJournalService.postConstruct();

        commitTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitTransactionDecisionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitRecoveredXidDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRecoveredXidDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        danglingTransactionsFoundEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

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
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction.getXid(),
                transaction.getUniqueNames()));
        XAPlusCommitTransactionDecisionLoggedEvent event = commitTransactionDecisionLoggedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(event.getXid(), transaction.getXid());
    }

    @Test
    public void testLogCommitTransactionDecisionFailed() throws InterruptedException, SQLException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        Mockito.doThrow(new SQLException("log_exception")).when(tlog)
                .log(transaction.getUniqueNames(), XAPlusTLog.TSTATUS.C);
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction.getXid(),
                transaction.getUniqueNames()));
        XAPlusCommitTransactionDecisionFailedEvent event = commitTransactionDecisionFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getXid());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusCommitTransactionDecisionLoggedEvent.Handler,
            XAPlusCommitTransactionDecisionFailedEvent.Handler,
            XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
            XAPlusCommitRecoveredXidDecisionLoggedEvent.Handler,
            XAPlusRollbackRecoveredXidDecisionLoggedEvent.Handler,
            XAPlusDanglingTransactionsFoundEvent.Handler {

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
        }

        @Override
        public void handleCommitTransactionDecisionLogged(XAPlusCommitTransactionDecisionLoggedEvent event)
                throws InterruptedException {
            commitTransactionDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleCommitTransactionDecisionFailed(XAPlusCommitTransactionDecisionFailedEvent event)
                throws InterruptedException {
            commitTransactionDecisionFailedEvents.put(event);
        }

        @Override
        public void handleRollbackTransactionDecisionLogged(XAPlusRollbackTransactionDecisionLoggedEvent event)
                throws InterruptedException {
            rollbackTransactionDecisionLoggedEvents.put(event);
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

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusCommitTransactionDecisionFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusCommitRecoveredXidDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRecoveredXidDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusDanglingTransactionsFoundEvent.class);
        }
    }
}
