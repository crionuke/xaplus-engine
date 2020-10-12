package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusPrepareTransactionEvent;
import org.xaplus.engine.events.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToPrepareEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusPrepareOrderWaiterServiceTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPrepareOrderWaiterServiceTest.class);

    XAPlusPrepareOrderWaiterService xaPlusPrepareOrderWaiterService;

    BlockingQueue<XAPlusPrepareTransactionEvent> waiterEvents;
    BlockingQueue<XAPlusRollbackRequestEvent> rollbackRequestEvents;
    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(1);

        xaPlusPrepareOrderWaiterService = new XAPlusPrepareOrderWaiterService(properties, threadPool, dispatcher);
        xaPlusPrepareOrderWaiterService.postConstruct();

        waiterEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusPrepareOrderWaiterService.finish();
        consumerStub.finish();
    }

    @Test
    public void testFirst2pcRequestAfterPrepareOrder() throws InterruptedException {
        XAPlusTransaction transaction1 = createSubordinateTransaction(XA_PLUS_RESOURCE_1);
        XAPlusTransaction transaction2 = createSubordinateTransaction(XA_PLUS_RESOURCE_2);
        // Send 2pc request
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction1));
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction2));
        // Send order to prepare only for one transaction
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToPrepareEvent(transaction2.getXid()));
        // Wating
        XAPlusPrepareTransactionEvent event = waiterEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(event.getTransaction().getXid(), transaction2.getXid());
    }

    @Test
    public void testFirstPrepareOrderAfter2pcRequest() throws InterruptedException {
        XAPlusTransaction transaction1 = createSubordinateTransaction(XA_PLUS_RESOURCE_1);
        XAPlusTransaction transaction2 = createSubordinateTransaction(XA_PLUS_RESOURCE_2);
        // Send order to prepare only for one transaction
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToPrepareEvent(transaction2.getXid()));
        // Send 2pc request
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction1));
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction2));
        // Wating
        XAPlusPrepareTransactionEvent event = waiterEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(event.getTransaction().getXid(), transaction2.getXid());
    }

    @Test
    public void testRemoteSuperiorOrderToRollback() throws InterruptedException {
        XAPlusTransaction transaction = createSubordinateTransaction(XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(transaction.getXid()));
        XAPlusRollbackRequestEvent event = rollbackRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusPrepareTransactionEvent.Handler,
            XAPlusRollbackRequestEvent.Handler {

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
        }

        @Override
        public void handlePrepareTransaction(XAPlusPrepareTransactionEvent event) throws InterruptedException {
            waiterEvents.put(event);
        }

        @Override
        public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
            rollbackRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusPrepareTransactionEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
        }
    }
}
