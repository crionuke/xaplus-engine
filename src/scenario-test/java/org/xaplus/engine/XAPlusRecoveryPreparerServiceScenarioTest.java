package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusDanglingTransactionsFoundEvent;
import org.xaplus.engine.events.journal.XAPlusFindDanglingTransactionsRequestEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveryPreparedEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveryRequestEvent;
import org.xaplus.engine.events.recovery.XAPlusRecoveryResourceRequestEvent;
import org.xaplus.engine.events.recovery.XAPlusResourceRecoveredEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusRecoveryPreparerServiceScenarioTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryPreparerServiceScenarioTest.class);

    XAPlusRecoveryPreparerService xaPlusRecoveryPreparerService;

    BlockingQueue<XAPlusFindDanglingTransactionsRequestEvent> findDanglingTransactionsRequestEvents;
    BlockingQueue<XAPlusRecoveryResourceRequestEvent> recoveryResourceRequestEvents;
    BlockingQueue<XAPlusRecoveryPreparedEvent> recoveryPreparedEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);

        xaPlusRecoveryPreparerService = new XAPlusRecoveryPreparerService(properties, threadPool, dispatcher, resources);
        xaPlusRecoveryPreparerService.postConstruct();

        findDanglingTransactionsRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        recoveryResourceRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        recoveryPreparedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusRecoveryPreparerService.finish();
        consumerStub.finish();
    }

    @Test
    public void testRecoveryPrepareFirstDangingNextResourcesRecoveryRequestsScenario() throws InterruptedException {
        // Test data set
        TestSuperiorDataSet dataSet = new TestSuperiorDataSet(XA_PLUS_RESOURCE_1);
        // Initiate server recovery
        dispatcher.dispatch(new XAPlusRecoveryRequestEvent());
        // First waiting danging transaction request
        waitingFindDanglingTransactionsRequest(dataSet);
        // Next waiting all resource recovery requests
        waitingResourceRecoveryRequests(dataSet);
        // Waiting recovery prepared event
        XAPlusRecoveryPreparedEvent recoveryPreparedEvent = recoveryPreparedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(recoveryPreparedEvent);
    }

    @Test
    public void testRecoveryPrepareFirstResourcesRecoveryNextDangingRequestScenario() throws InterruptedException {
        // Test data set
        TestSuperiorDataSet dataSet = new TestSuperiorDataSet(XA_PLUS_RESOURCE_1);
        // Initiate prepare recovery
        dispatcher.dispatch(new XAPlusRecoveryRequestEvent());
        // First waiting all resource recovery requests
        waitingResourceRecoveryRequests(dataSet);
        // Next waiting danging transaction request
        waitingFindDanglingTransactionsRequest(dataSet);
        // Waiting recovery prepared event
        XAPlusRecoveryPreparedEvent recoveryPreparedEvent = recoveryPreparedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(recoveryPreparedEvent);
    }

    private void waitingFindDanglingTransactionsRequest(TestSuperiorDataSet dataSet) throws InterruptedException {
        // Waiting find dangling transaction request
        XAPlusFindDanglingTransactionsRequestEvent findDanglingTransactionsRequestEvent =
                findDanglingTransactionsRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(findDanglingTransactionsRequestEvent);
        dispatcher.dispatch(new XAPlusDanglingTransactionsFoundEvent(dataSet.allDanglingTransactions));
    }

    private void waitingResourceRecoveryRequests(TestSuperiorDataSet dataSet) throws InterruptedException {
        // Waiting recovery requests for XA resources
        for (int i = 0; i < dataSet.allPreparedXids.size(); i++) {
            XAPlusRecoveryResourceRequestEvent recoveryResourceRequestEvent =
                    recoveryResourceRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            assertNotNull(recoveryResourceRequestEvent);
            String uniqueName = recoveryResourceRequestEvent.getUniqueName();
            if (dataSet.allPreparedXids.containsKey(uniqueName)) {
                // Response
                dispatcher.dispatch(new XAPlusResourceRecoveredEvent(uniqueName,
                        dataSet.allPreparedXids.get(uniqueName)));
            } else {
                fail("unknown resource name=" + uniqueName + " to recovery");
            }
        }
    }

    private class ConsumerStub extends Bolt implements
            XAPlusFindDanglingTransactionsRequestEvent.Handler,
            XAPlusRecoveryResourceRequestEvent.Handler,
            XAPlusRecoveryPreparedEvent.Handler {

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handleFindDanglingTransactionsRequest(XAPlusFindDanglingTransactionsRequestEvent event) throws InterruptedException {
            findDanglingTransactionsRequestEvents.put(event);
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
            dispatcher.subscribe(this, XAPlusFindDanglingTransactionsRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveryResourceRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveryPreparedEvent.class);
        }
    }
}
