package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusCommitTransactionEvent;
import org.xaplus.engine.events.XAPlusReportReadyStatusRequestEvent;
import org.xaplus.engine.events.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.XAPlusTransactionPreparedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusCommitOrderWaiterServiceTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusCommitOrderWaiterServiceTest.class);

    XAPlusCommitOrderWaiterService xaPlusCommitOrderWaiterService;

    BlockingQueue<XAPlusReportReadyStatusRequestEvent> reportReadyStatusRequestEvents;
    BlockingQueue<XAPlus2pcFailedEvent> twoPcFailedEvents;
    BlockingQueue<XAPlusCommitTransactionEvent> commitTransactionEvents;
    BlockingQueue<XAPlusRollbackRequestEvent> rollbackRequestEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_2);

        xaPlusCommitOrderWaiterService =
                new XAPlusCommitOrderWaiterService(properties, threadPool, dispatcher, resources);
        xaPlusCommitOrderWaiterService.postConstruct();

        reportReadyStatusRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        twoPcFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitTransactionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusCommitOrderWaiterService.finish();
        consumerStub.finish();
    }

    @Test
    public void testTransactionPreparedSuccessfully() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_2);
        dispatcher.dispatch(new XAPlusTransactionPreparedEvent(transaction));
        XAPlusReportReadyStatusRequestEvent event =
                reportReadyStatusRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getXid());
    }

    @Test
    public void testTransactionPreparedFailed() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction("unknown_server",
                XA_PLUS_RESOURCE_2);
        dispatcher.dispatch(new XAPlusTransactionPreparedEvent(transaction));
        XAPlus2pcFailedEvent event = twoPcFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void testRemoteSuperiorOrderToCommit() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_2);
        dispatcher.dispatch(new XAPlusTransactionPreparedEvent(transaction));
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToCommitEvent(transaction.getXid()));
        XAPlusCommitTransactionEvent event = commitTransactionEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void testRemoteSuperiorOrderToRollback() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_2);
        dispatcher.dispatch(new XAPlusTransactionPreparedEvent(transaction));
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(transaction.getXid()));
        XAPlusRollbackRequestEvent event = rollbackRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusReportReadyStatusRequestEvent.Handler,
            XAPlus2pcFailedEvent.Handler,
            XAPlusCommitTransactionEvent.Handler,
            XAPlusRollbackRequestEvent.Handler {

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
        }

        @Override
        public void handleReportReadyStatusRequest(XAPlusReportReadyStatusRequestEvent event)
                throws InterruptedException {
            reportReadyStatusRequestEvents.put(event);
        }

        @Override
        public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
            twoPcFailedEvents.put(event);
        }

        @Override
        public void handleCommitTransaction(XAPlusCommitTransactionEvent event) throws InterruptedException {
            commitTransactionEvents.put(event);
        }

        @Override
        public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
            rollbackRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusReportReadyStatusRequestEvent.class);
            dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
            dispatcher.subscribe(this, XAPlusCommitTransactionEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
        }
    }
}
