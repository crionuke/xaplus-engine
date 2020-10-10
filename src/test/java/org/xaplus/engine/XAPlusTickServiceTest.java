package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.xaplus.engine.events.XAPlusTickEvent;
import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusTickServiceTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTickServiceTest.class);

    XAPlusTickService xaPlusTickService;

    BlockingQueue<XAPlusTickEvent> tickEvents;
    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents();

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
        assertEquals(tick1.getIndex(), 1);
        assertNotNull(tick2);
        assertEquals(tick2.getIndex(), 2);
        assertNotNull(tick3);
        assertEquals(tick3.getIndex(), 3);
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
