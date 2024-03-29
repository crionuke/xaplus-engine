package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;
import org.xaplus.engine.events.recovery.XAPlusPrepareRecoveryRequestEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionClosedEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.user.XAPlusUserCreateTransactionEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusManagerServiceUnitTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusManagerServiceUnitTest.class);

    private XAPlusManagerService xaPlusManagerService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        // Setup with recovery period
        createXAPlusComponents(XA_PLUS_RESOURCE_1, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);
        xaPlusManagerService = new XAPlusManagerService(properties, threadPool, dispatcher);
        xaPlusManagerService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusManagerService.finish();
        consumerStub.finish();
    }

    @Test
    public void testTransactionDone() throws InterruptedException, XAPlusCommitException,
            XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        XAPlusTransactionClosedEvent event =
                consumerStub.transactionClosedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
        assertTrue(transaction.getFuture().getResult());
    }

    @Test
    public void testTransactionRolledBack() throws InterruptedException, XAPlusCommitException,
            XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
        XAPlusTransactionClosedEvent event =
                consumerStub.transactionClosedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
        assertFalse(transaction.getFuture().getResult());
    }

    @Test(expected = XAPlusCommitException.class)
    public void test2pcFailed() throws InterruptedException, XAPlusCommitException,
            XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction));
        XAPlusTransactionClosedEvent event =
                consumerStub.transactionClosedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
        transaction.getFuture().getResult();
    }

    @Test(expected = XAPlusRollbackException.class)
    public void testRollbackFailed() throws InterruptedException, XAPlusCommitException,
            XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction));
        XAPlusTransactionClosedEvent event =
                consumerStub.transactionClosedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
        assertFalse(transaction.getFuture().getResult());
    }

    @Test(expected = XAPlusTimeoutException.class)
    public void test2pcTimeout() throws InterruptedException, XAPlusCommitException,
            XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // Wait timeout
        Thread.sleep(properties.getTransactionsTimeoutInSeconds() * 1000 + POLL_TIMIOUT_MS);
        dispatcher.dispatch(new XAPlusTickEvent(1));
        XAPlusTransactionTimedOutEvent event1 =
                consumerStub.transactionTimedOutEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction, event1.getTransaction());
        XAPlusTransactionClosedEvent event2 =
                consumerStub.transactionClosedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction, event2.getTransaction());
        assertFalse(transaction.getFuture().getResult());
    }

    @Test(expected = XAPlusTimeoutException.class)
    public void testRollbackTimeout() throws InterruptedException, XAPlusCommitException,
            XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // Wait timeout
        Thread.sleep(properties.getTransactionsTimeoutInSeconds() * 1000 + POLL_TIMIOUT_MS);
        dispatcher.dispatch(new XAPlusTickEvent(1));
        XAPlusTransactionTimedOutEvent event1 =
                consumerStub.transactionTimedOutEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction, event1.getTransaction());
        XAPlusTransactionClosedEvent event2 =
                consumerStub.transactionClosedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction, event2.getTransaction());
        assertFalse(transaction.getFuture().getResult());
    }

    @Test
    public void testRecoveryPeriods() throws InterruptedException {
        // Period 1
        Thread.sleep(properties.getRecoveryPeriodInSeconds() * 1000 + POLL_TIMIOUT_MS);
        dispatcher.dispatch(new XAPlusTickEvent(1));
        XAPlusPrepareRecoveryRequestEvent event2 =
                consumerStub.prepareRecoveryRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        // Period 2
        Thread.sleep(properties.getRecoveryPeriodInSeconds() * 1000 + POLL_TIMIOUT_MS);
        dispatcher.dispatch(new XAPlusTickEvent(2));
        XAPlusPrepareRecoveryRequestEvent event3 =
                consumerStub.prepareRecoveryRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event3);
    }

    private class ConsumerStub extends Bolt implements
            XAPlusTransactionClosedEvent.Handler,
            XAPlusTransactionTimedOutEvent.Handler,
            XAPlusPrepareRecoveryRequestEvent.Handler {

        BlockingQueue<XAPlusTransactionClosedEvent> transactionClosedEvents;
        BlockingQueue<XAPlusTransactionTimedOutEvent> transactionTimedOutEvents;
        BlockingQueue<XAPlusPrepareRecoveryRequestEvent> prepareRecoveryRequestEvents;

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
            transactionClosedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            transactionTimedOutEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            prepareRecoveryRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleTransactionClosed(XAPlusTransactionClosedEvent event) throws InterruptedException {
            transactionClosedEvents.put(event);
        }

        @Override
        public void handleTransactionTimedOut(XAPlusTransactionTimedOutEvent event) throws InterruptedException {
            transactionTimedOutEvents.put(event);
        }

        @Override
        public void handlePrepareRecoveryRequest(XAPlusPrepareRecoveryRequestEvent event) throws InterruptedException {
            prepareRecoveryRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusTransactionClosedEvent.class);
            dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
            dispatcher.subscribe(this, XAPlusPrepareRecoveryRequestEvent.class);
        }
    }
}
