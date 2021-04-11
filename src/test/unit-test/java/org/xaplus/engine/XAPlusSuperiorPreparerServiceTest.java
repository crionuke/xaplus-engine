package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.rollback.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.tm.XAPlusTransactionClosedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class XAPlusSuperiorPreparerServiceTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSuperiorPreparerServiceTest.class);

    private XAPlusSuperiorPreparerService xaPlusSuperiorPreparerService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        xaPlusSuperiorPreparerService = new XAPlusSuperiorPreparerService(properties, threadPool, dispatcher,
                new XAPlusTracker());
        xaPlusSuperiorPreparerService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusSuperiorPreparerService.finish();
        consumerStub.finish();
    }


    private class ConsumerStub extends Bolt implements
            XAPlusTransactionClosedEvent.Handler,
            XAPlus2pcRequestEvent.Handler,
            XAPlusRollbackRequestEvent.Handler {

        BlockingQueue<XAPlusTransactionClosedEvent> transactionClosedEvents;
        BlockingQueue<XAPlus2pcRequestEvent> twoPcRequestEvents;
        BlockingQueue<XAPlusRollbackRequestEvent> rollbackRequestEvents;

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
            transactionClosedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            twoPcRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleTransactionClosed(XAPlusTransactionClosedEvent event) throws InterruptedException {
            transactionClosedEvents.put(event);
        }

        @Override
        public void handle2pcRequest(XAPlus2pcRequestEvent event) throws InterruptedException {
            twoPcRequestEvents.put(event);
        }

        @Override
        public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
            rollbackRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusTransactionClosedEvent.class);
            dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
        }
    }
}
