package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusReportTransactionStatusRequestEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.timer.XAPlusTransactionTimedOutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusManagerServiceTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusManagerServiceTest.class);

    XAPlusManagerService xaPlusManagerService;

    BlockingQueue<XAPlus2pcRequestEvent> twoPcRequestEvents;
    BlockingQueue<XAPlusRollbackRequestEvent> rollbackRequestEvents;
    BlockingQueue<XAPlusReportTransactionStatusRequestEvent> reportTransactionStatusRequestEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);

        xaPlusManagerService = new XAPlusManagerService(properties, threadPool, dispatcher, resources);
        xaPlusManagerService.postConstruct();

        twoPcRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        reportTransactionStatusRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusManagerService.finish();
        consumerStub.finish();
    }

    @Test
    public void testUserCommitRequest() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        XAPlus2pcRequestEvent event = twoPcRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void testUserRollbackRequest() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        XAPlusRollbackRequestEvent event = rollbackRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void test2pcDone()
            throws InterruptedException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        assertTrue(transaction.getFuture().get());
    }

    @Test
    public void testRollbackDone()
            throws InterruptedException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
        assertTrue(transaction.getFuture().get());
    }

//    @Test(expected = XAPlusCommitException.class)
//    public void test2pcFailed()
//            throws InterruptedException, XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
//        XAPlusTransaction transaction = createTestSuperiorTransaction();
//        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
//        dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction, new Exception("commit_exception")));
//        transaction.getFuture().get();
//    }

    @Test(expected = XAPlusRollbackException.class)
    public void testRollbackFailed()
            throws InterruptedException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction));
        transaction.getFuture().get();
    }

    @Test(expected = XAPlusTimeoutException.class)
    public void testTimeout()
            throws InterruptedException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusTransactionTimedOutEvent(transaction));
        transaction.getFuture().get();
    }

    @Test
    public void testRemoteSuperiorOrderToCommit() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToCommitEvent(transaction.getXid()));
        XAPlusReportTransactionStatusRequestEvent reportTransactionStatusRequestEvent =
                reportTransactionStatusRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(reportTransactionStatusRequestEvent);
        assertEquals(transaction.getXid(), reportTransactionStatusRequestEvent.getXid());
    }

    @Test
    public void testRemoteSuperiorOrderToRollback() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(transaction.getXid()));
        XAPlusReportTransactionStatusRequestEvent reportTransactionStatusRequestEvent =
                reportTransactionStatusRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(reportTransactionStatusRequestEvent);
        assertEquals(transaction.getXid(), reportTransactionStatusRequestEvent.getXid());
    }

    private class ConsumerStub extends Bolt implements
            XAPlus2pcRequestEvent.Handler,
            XAPlusRollbackRequestEvent.Handler,
            XAPlusReportTransactionStatusRequestEvent.Handler {


        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handle2pcRequest(XAPlus2pcRequestEvent event) throws InterruptedException {
            twoPcRequestEvents.put(event);
        }

        @Override
        public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
            rollbackRequestEvents.put(event);
        }

        @Override
        public void handleReportTransactionStatusRequest(XAPlusReportTransactionStatusRequestEvent event)
                throws InterruptedException {
            reportTransactionStatusRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
            dispatcher.subscribe(this, XAPlusReportTransactionStatusRequestEvent.class);
        }
    }
}
