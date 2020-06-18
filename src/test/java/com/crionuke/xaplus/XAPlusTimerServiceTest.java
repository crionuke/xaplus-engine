package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.*;
import com.crionuke.xaplus.events.twopc.XAPlus2pcDoneEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcFailedEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcRequestEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusTimerServiceTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTimerServiceTest.class);

    XAPlusTimerService xaPlusTimerService;

    BlockingQueue<XAPlusTimeoutEvent> timeoutEvents;
    BlockingQueue<XAPlusTimerCancelledEvent> timerCancelledEvents;
    TimerEventConsumer timerEventConsumer;

    @Before
    public void beforeTest() {
        createXAPlusComponents(1);

        xaPlusTimerService = new XAPlusTimerService(properties, threadPool, dispatcher);
        xaPlusTimerService.postConstruct();

        timeoutEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        timerCancelledEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        timerEventConsumer = new TimerEventConsumer();
        timerEventConsumer.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusTimerService.finish();
        timerEventConsumer.finish();
    }

    @Test
    public void test2pcTimeout() throws InterruptedException {
        int timeout = 1000;
        XAPlusTransaction transaction = createTransaction(timeout / 1000);
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        Thread.sleep(timeout);
        dispatcher.dispatch(new XAPlusTickEvent(1));
        XAPlusTimeoutEvent timeoutEvent = timeoutEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timeoutEvent);
        assertEquals(timeoutEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("transaction {} timed out", timeoutEvent.getTransaction());
    }

    @Test
    public void testRollbackTimeout() throws InterruptedException {
        int timeout = 1000;
        XAPlusTransaction transaction = createTransaction(timeout / 1000);
        dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        Thread.sleep(timeout);
        dispatcher.dispatch(new XAPlusTickEvent(1));
        XAPlusTimeoutEvent timeoutEvent = timeoutEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timeoutEvent);
        assertEquals(timeoutEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("transaction {} timed out", timeoutEvent.getTransaction());
    }

    @Test
    public void test2pcRequestDone() throws InterruptedException  {
        XAPlusTransaction transaction = createTransaction();
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        XAPlusTimerCancelledEvent timerCancelledEvent = timerCancelledEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timerCancelledEvent);
        assertEquals(timerCancelledEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("transaction {} done", timerCancelledEvent.getTransaction());
    }

    @Test
    public void test2pcRequestFailed() throws InterruptedException  {
        XAPlusTransaction transaction = createTransaction();
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction, new Exception("2pc failed")));
        XAPlusTimerCancelledEvent timerCancelledEvent = timerCancelledEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timerCancelledEvent);
        assertEquals(timerCancelledEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("transaction {} failed", timerCancelledEvent.getTransaction());
    }

    @Test
    public void testRollbackRequestDone() throws InterruptedException  {
        XAPlusTransaction transaction = createTransaction();
        dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        XAPlusTimerCancelledEvent timerCancelledEvent = timerCancelledEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timerCancelledEvent);
        assertEquals(timerCancelledEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("rolling back of transaction {} done", timerCancelledEvent.getTransaction());
    }

    @Test
    public void testRollbackRequestFailed() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction();
        dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction,  new Exception("rollback failed")));
        XAPlusTimerCancelledEvent timerCancelledEvent = timerCancelledEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(timerCancelledEvent);
        assertEquals(timerCancelledEvent.getTransaction().getXid(), transaction.getXid());
        logger.info("rolling back of transaction {} failed", timerCancelledEvent.getTransaction());
    }

    private class TimerEventConsumer extends Bolt implements
            XAPlusTimeoutEvent.Handler,
            XAPlusTimerCancelledEvent.Handler {

        TimerEventConsumer() {
            super("timer-event-consumer", QUEUE_SIZE);
        }

        @Override
        public void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException {
            timeoutEvents.put(event);
        }

        @Override
        public void handleTimerCancelled(XAPlusTimerCancelledEvent event) throws InterruptedException {
            timerCancelledEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusTimeoutEvent.class);
            dispatcher.subscribe(this, XAPlusTimerCancelledEvent.class);
        }
    }
}
