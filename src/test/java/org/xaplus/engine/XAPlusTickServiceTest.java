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

public class XAPlusTickServiceTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTickServiceTest.class);

    XAPlusTickService xaPlusTickService;

    BlockingQueue<XAPlusTickEvent> tickEvents;
    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(SERVER_ID_DEFAULT);

        xaPlusTickService = new XAPlusTickService(threadPool, dispatcher);
        xaPlusTickService.postConstruct();

        tickEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
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
        XAPlusTickEvent tick1 = tickEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        XAPlusTickEvent tick2 = tickEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        XAPlusTickEvent tick3 = tickEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(tick1);
        assertTrue(tick1.getIndex() < tick2.getIndex());
        assertNotNull(tick2);
        assertTrue(tick2.getIndex() < tick3.getIndex());
        assertNotNull(tick3);
    }

    private class ConsumerStub extends Bolt implements XAPlusTickEvent.Handler {

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
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
