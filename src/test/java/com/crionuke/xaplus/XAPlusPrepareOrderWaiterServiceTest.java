package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.XAPlusPrepareTransactionEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcRequestEvent;
import com.crionuke.xaplus.events.xaplus.XAPlusRemoteSuperiorOrderToPrepareEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusPrepareOrderWaiterServiceTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPrepareOrderWaiterServiceTest.class);

    XAPlusPrepareOrderWaiterService xaPlusPrepareOrderWaiterService;

    BlockingQueue<XAPlusPrepareTransactionEvent> waiterEvents;
    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(1);

        xaPlusPrepareOrderWaiterService = new XAPlusPrepareOrderWaiterService(properties, threadPool, dispatcher);
        xaPlusPrepareOrderWaiterService.postConstruct();

        waiterEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

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
        XAPlusTransaction transaction1 = createSubordinateTransaction();
        XAPlusTransaction transaction2 = createSubordinateTransaction();
        XAPlusTransaction transaction3 = createSubordinateTransaction();
        // Send 2pc request
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction1));
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction2));
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction3));
        // Send order to prepare only for one transaction
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToPrepareEvent(transaction2.getXid()));
        // Wating
        XAPlusPrepareTransactionEvent event = waiterEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(event.getTransaction().getXid(), transaction2.getXid());
    }

    @Test
    public void testFirstPrepareOrderAfter2pcRequest() throws InterruptedException {
        XAPlusTransaction transaction1 = createSubordinateTransaction();
        XAPlusTransaction transaction2 = createSubordinateTransaction();
        XAPlusTransaction transaction3 = createSubordinateTransaction();
        // Send order to prepare only for one transaction
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToPrepareEvent(transaction2.getXid()));
        // Send 2pc request
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction1));
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction2));
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction3));
        // Wating
        XAPlusPrepareTransactionEvent event = waiterEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(event.getTransaction().getXid(), transaction2.getXid());
    }

    private class ConsumerStub extends Bolt implements XAPlusPrepareTransactionEvent.Handler {

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
        }

        @Override
        public void handlePrepareTransaction(XAPlusPrepareTransactionEvent event) throws InterruptedException {
            waiterEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusPrepareTransactionEvent.class);
        }
    }
}
