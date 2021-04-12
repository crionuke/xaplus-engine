package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.xaplus.XAPlusRetryCommitOrderRequestEvent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusRecoveryPreparerServiceTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryPreparerServiceTest.class);

    XAPlusRecoveryPreparerService xaPlusRecoveryPreparerService;
    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        xaPlusRecoveryPreparerService =
                new XAPlusRecoveryPreparerService(properties, threadPool, dispatcher, resources);
        xaPlusRecoveryPreparerService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusRecoveryPreparerService.finish();
        consumerStub.finish();
    }

    @Test
    public void testRecoveryPreparer() throws InterruptedException {
        // Start recovery
        dispatcher.dispatch(new XAPlusPrepareRecoveryRequestEvent(System.currentTimeMillis()));
        Set<String> waitedResources = new HashSet<>();
        waitedResources.add(XA_RESOURCE_1);
        waitedResources.add(XA_RESOURCE_2);
        waitedResources.add(XA_RESOURCE_3);
        Map<String, XAPlusRecoveredResource> recoveredResources = new HashMap<>();
        int count = waitedResources.size();
        for (int iteration = 0; iteration < count; iteration++) {
            XAPlusRecoveryResourceRequestEvent event1 = consumerStub
                    .recoveryResourceRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            assertNotNull(event1);
            XAPlusRecoveredResource recoveredResource = event1.getRecoveredResource();
            String uniqueName = recoveredResource.getUniqueName();
            waitedResources.remove(uniqueName);
            recoveredResources.put(uniqueName, recoveredResource);
        }
        assertTrue(waitedResources.isEmpty());
        // Simulate recovery
        dispatcher.dispatch(new XAPlusResourceRecoveredEvent(recoveredResources.get(XA_RESOURCE_1)));
        dispatcher.dispatch(new XAPlusRecoveryResourceFailedEvent(recoveredResources.get(XA_RESOURCE_2),
                new Exception("recovery_exception")));
        dispatcher.dispatch(new XAPlusResourceRecoveredEvent(recoveredResources.get(XA_RESOURCE_3)));
        // Check final event
        XAPlusRecoveryPreparedEvent event2 = consumerStub
                .recoveryPreparedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        Set<XAPlusRecoveredResource> finalRecoveredResources = event2.getRecoveredResources();
        assertEquals(2, finalRecoveredResources.size());
        assertTrue(finalRecoveredResources.contains(recoveredResources.get(XA_RESOURCE_1)));
        assertTrue(finalRecoveredResources.contains(recoveredResources.get(XA_RESOURCE_3)));
    }

    private class ConsumerStub extends Bolt implements
            XAPlusRecoveryResourceRequestEvent.Handler,
            XAPlusRecoveryPreparedEvent.Handler {

        BlockingQueue<XAPlusRecoveryResourceRequestEvent> recoveryResourceRequestEvents;
        BlockingQueue<XAPlusRecoveryPreparedEvent> recoveryPreparedEvents;

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
            recoveryResourceRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            recoveryPreparedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleRecoveryResourceRequest(XAPlusRecoveryResourceRequestEvent event) throws InterruptedException {
            recoveryResourceRequestEvents.put(event);
        }

        @Override
        public void handleRecoveryPrepared(XAPlusRecoveryPreparedEvent event) throws InterruptedException {
            recoveryPreparedEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusRecoveryResourceRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveryPreparedEvent.class);
        }
    }
}
