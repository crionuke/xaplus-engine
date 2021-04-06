package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.timer.XAPlusTimerCancelledEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusTimerServiceTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTimerServiceTest.class);

    private XAPlusTimerService xaPlusTimerService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);

        xaPlusTimerService = new XAPlusTimerService(properties, threadPool, dispatcher, new XAPlusTimerState());
        xaPlusTimerService.postConstruct();

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusTimerService.finish();
        consumerStub.finish();
    }

    @Test
    public void test2pcTimeout() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        Thread.sleep(properties.getDefaultTimeoutInSeconds() * 1000);
        dispatcher.dispatch(new XAPlusTickEvent(1));
        XAPlusTransactionTimedOutEvent timeoutEvent =
                consumerStub.timeoutEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timeoutEvent);
        assertEquals(timeoutEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("Transaction {} timed out", timeoutEvent.getTransaction());
    }

    @Test
    public void testRollbackTimeout() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        Thread.sleep(properties.getDefaultTimeoutInSeconds() * 1000);
        dispatcher.dispatch(new XAPlusTickEvent(1));
        XAPlusTransactionTimedOutEvent timeoutEvent =
                consumerStub.timeoutEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timeoutEvent);
        assertEquals(timeoutEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("Transaction {} timed out", timeoutEvent.getTransaction());
    }

    @Test
    public void test2pcRequestDone() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        XAPlusTimerCancelledEvent timerCancelledEvent =
                consumerStub.timerCancelledEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timerCancelledEvent);
        assertEquals(timerCancelledEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("Transaction {} done", timerCancelledEvent.getTransaction());
    }

    @Test
    public void test2pcRequestFailed() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction));
        XAPlusTimerCancelledEvent timerCancelledEvent = consumerStub.timerCancelledEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timerCancelledEvent);
        assertEquals(timerCancelledEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("Transaction {} failed", timerCancelledEvent.getTransaction());
    }

    @Test
    public void testRollbackRequestDone() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        XAPlusTimerCancelledEvent timerCancelledEvent = consumerStub.timerCancelledEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timerCancelledEvent);
        assertEquals(timerCancelledEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("rolling back of transaction {} done", timerCancelledEvent.getTransaction());
    }

    @Test
    public void testRollbackRequestFailed() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction));
        XAPlusTimerCancelledEvent timerCancelledEvent = consumerStub.timerCancelledEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timerCancelledEvent);
        assertEquals(timerCancelledEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("rolling back of transaction {} failed", timerCancelledEvent.getTransaction());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusTransactionTimedOutEvent.Handler,
            XAPlusTimerCancelledEvent.Handler {

        BlockingQueue<XAPlusTransactionTimedOutEvent> timeoutEvents;
        BlockingQueue<XAPlusTimerCancelledEvent> timerCancelledEvents;

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);

            timeoutEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            timerCancelledEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleTransactionTimedOut(XAPlusTransactionTimedOutEvent event) throws InterruptedException {
            timeoutEvents.put(event);
        }

        @Override
        public void handleTimerCancelled(XAPlusTimerCancelledEvent event) throws InterruptedException {
            timerCancelledEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusTransactionTimedOutEvent.class);
            dispatcher.subscribe(this, XAPlusTimerCancelledEvent.class);
        }
    }
}
