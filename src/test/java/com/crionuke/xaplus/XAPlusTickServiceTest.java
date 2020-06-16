package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.XAPlusTickEvent;
import org.junit.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusTickServiceTest extends Assert {
    final int QUEUE_SIZE = 128;
    final int POLL_TIMIOUT_MS = 2000;

    XAPlusThreadPool xaPlusThreadPool;
    XAPlusDispatcher xaPlusDispatcher;
    XAPlusTickService xaPlusTickService;

    BlockingQueue<XAPlusTickEvent> tickEvents;
    TickEventConsumer tickEventConsumer;

    @Before
    public void beforeTest() {
        xaPlusThreadPool = new XAPlusThreadPool();
        xaPlusDispatcher = new XAPlusDispatcher();
        xaPlusTickService = new XAPlusTickService(xaPlusThreadPool, xaPlusDispatcher);
        xaPlusTickService.postConstruct();

        tickEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        tickEventConsumer = new TickEventConsumer(tickEvents, xaPlusThreadPool, xaPlusDispatcher);
        tickEventConsumer.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusTickService.finish();
        tickEventConsumer.finish();
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

    class TickEventConsumer extends Bolt
            implements XAPlusTickEvent.Handler {

        private final BlockingQueue<XAPlusTickEvent> tickEvents;

        TickEventConsumer(BlockingQueue<XAPlusTickEvent> tickEvents, XAPlusThreadPool xaPlusThreadPool,
                          XAPlusDispatcher xaPlusDispatcher) {
            super("tick-event-consumer", QUEUE_SIZE);
            this.tickEvents = tickEvents;
        }

        @Override
        public void handleTick(XAPlusTickEvent event) throws InterruptedException {
            tickEvents.put(event);
        }

        void postConstruct() {
            xaPlusThreadPool.execute(this);
            xaPlusDispatcher.subscribe(this, XAPlusTickEvent.class);
        }
    }
}
