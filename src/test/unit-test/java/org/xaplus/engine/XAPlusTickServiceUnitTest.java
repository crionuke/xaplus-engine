package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTickEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusTickServiceUnitTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTickServiceUnitTest.class);

    private XAPlusTickService xaPlusTickService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        xaPlusTickService = new XAPlusTickService(properties, threadPool, dispatcher);
        xaPlusTickService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusTickService.finish();
        consumerStub.finish();
    }

    @Test
    public void testTickEvents() throws InterruptedException {
        XAPlusTickEvent tick1 = consumerStub.tickEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        XAPlusTickEvent tick2 = consumerStub.tickEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        XAPlusTickEvent tick3 = consumerStub.tickEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(tick1);
        assertTrue(tick1.getIndex() < tick2.getIndex());
        assertNotNull(tick2);
        assertTrue(tick2.getIndex() < tick3.getIndex());
        assertNotNull(tick3);
    }

    private class ConsumerStub extends Bolt implements XAPlusTickEvent.Handler {

        BlockingQueue<XAPlusTickEvent> tickEvents;

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
            tickEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleTick(XAPlusTickEvent event) throws InterruptedException {
            tickEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusTickEvent.class);
        }
    }
}
