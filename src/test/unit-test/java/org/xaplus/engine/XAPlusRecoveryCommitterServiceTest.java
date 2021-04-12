package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.xaplus.XAPlusRetryCommitOrderRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRetryFromSuperiorRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRetryRollbackOrderRequestEvent;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class XAPlusRecoveryCommitterServiceTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryCommitterServiceTest.class);

    XAPlusRecoveryCommitterService xaPlusRecoveryCommitterService;

    BlockingQueue<XAPlusRetryCommitOrderRequestEvent> retryCommitOrderRequestEvents;
    BlockingQueue<XAPlusRetryRollbackOrderRequestEvent> retryRollbackOrderRequestEvents;

    BlockingQueue<XAPlusCommitRecoveredXidRequestEvent> commitRecoveredXidRequestEvents;
    BlockingQueue<XAPlusRollbackRecoveredXidRequestEvent> rollbackRecoveredXidRequestEvents;
    BlockingQueue<XAPlusRetryFromSuperiorRequestEvent> retryFromSuperiorRequestEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);

        xaPlusRecoveryCommitterService =
                new XAPlusRecoveryCommitterService(properties, threadPool, dispatcher, resources);
        xaPlusRecoveryCommitterService.postConstruct();

        retryCommitOrderRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        retryRollbackOrderRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitRecoveredXidRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRecoveredXidRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        retryFromSuperiorRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusRecoveryCommitterService.finish();
        consumerStub.finish();
    }

//    @Test
//    public void testRecoveryRetryOrdersFromSuperiorSide() throws InterruptedException {
//        // Test transactions data set
//        TestSuperiorDataSet dataSet = new TestSuperiorDataSet(XA_PLUS_RESOURCE_1);
//        // Initiate prepare recovery
//        dispatcher.dispatch(new XAPlusRecoveryPreparedEvent(new HashMap<>(), new HashMap<>(), new HashMap<>(),
//                new HashMap<>(), dataSet.allDanglingTransactions));
//        // Waiting retry orders requests for XA+ resources
//        Set<XAPlusXid> orders = new HashSet<>();
//        dataSet.xaPlusDanglingXids.keySet().forEach((xid) -> orders.add(xid));
//        for (Map.Entry<XAPlusXid, Boolean> entry : dataSet.xaPlusDanglingXids.entrySet()) {
//            Boolean status = entry.getValue();
//            if (status) {
//                XAPlusRetryCommitOrderRequestEvent retryCommitOrderRequestEvent =
//                        retryCommitOrderRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//                assertNotNull(retryCommitOrderRequestEvent);
//                XAPlusXid xid = retryCommitOrderRequestEvent.getXid();
//                assertTrue(orders.remove(xid));
//                // Response done
//                dispatcher.dispatch(new XAPlusRemoteSubordinateDoneEvent(xid));
//                // Waiting event that xid committed
//                XAPlusDanglingTransactionCommittedEvent danglingTransactionCommittedEvent =
//                        danglingTransactionCommittedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//                assertNotNull(danglingTransactionCommittedEvent);
//                assertEquals(xid, danglingTransactionCommittedEvent.getXid());
//            } else {
//                XAPlusRetryRollbackOrderRequestEvent retryRollbackOrderRequestEvent =
//                        retryRollbackOrderRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//                assertNotNull(retryRollbackOrderRequestEvent);
//                XAPlusXid xid = retryRollbackOrderRequestEvent.getXid();
//                assertTrue(orders.remove(xid));
//                // Response done
//                dispatcher.dispatch(new XAPlusRemoteSubordinateDoneEvent(xid));
//                // Waiting event that xid rolledback
//                XAPlusDanglingTransactionRolledBackEvent danglingTransactionRolledBackEvent =
//                        danglingTransactionRolledBackEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//                assertNotNull(danglingTransactionRolledBackEvent);
//                assertEquals(xid, danglingTransactionRolledBackEvent.getXid());
//            }
//        }
//        assertTrue(orders.isEmpty());
//    }
//
//    @Test
//    public void testRecoveryXAResourcesOnSuperiorSide() throws InterruptedException {
//        // Test transactions data set
//        TestSuperiorDataSet dataSet = new TestSuperiorDataSet(XA_PLUS_RESOURCE_1);
//        // Test xa resources
//        TestXAResources xaResources = new TestXAResources();
//        // Initiate prepare recovery
//        dispatcher.dispatch(new XAPlusRecoveryPreparedEvent(xaResources.getJdbcConnections(),
//                xaResources.getJmsContexts(), xaResources.getXaResources(), dataSet.allPreparedXids,
//                dataSet.xaDanglingTransactions));
//        // Waiting commit requests
//        List<XAPlusXid> committedXid = dataSet.xaDanglingXids.entrySet().stream()
//                .filter((entry) -> entry.getValue())
//                .map((entry) -> entry.getKey())
//                .collect(Collectors.toList());
//        for (int i = 0; i < committedXid.size(); i++) {
//            XAPlusCommitRecoveredXidRequestEvent commitRecoveredXidRequestEvent =
//                    commitRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//            assertNotNull(commitRecoveredXidRequestEvent);
//            assertTrue(committedXid.contains(commitRecoveredXidRequestEvent.getXid()));
//        }
//        // Waiting rollback requests
//        List<XAPlusXid> rolledBackXid = dataSet.xaDanglingXids.entrySet().stream()
//                .filter((entry) -> !entry.getValue())
//                .map((entry) -> entry.getKey())
//                .collect(Collectors.toList());
//        for (int i = 0; i < rolledBackXid.size() + dataSet.xaNoDecisionXids.size(); i++) {
//            XAPlusRollbackRecoveredXidRequestEvent rollbackRecoveredXidRequestEvent =
//                    rollbackRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//            assertNotNull(rollbackRecoveredXidRequestEvent);
//            XAPlusXid xid = rollbackRecoveredXidRequestEvent.getXid();
//            assertTrue(rolledBackXid.contains(xid) || dataSet.xaNoDecisionXids.contains(xid));
//        }
//    }
//
//    @Test
//    public void testRecoveryXAResourcesOnSubordinateSide() throws InterruptedException {
//        // Test transactions data set
//        TestSubordinateDataSet dataSet = new TestSubordinateDataSet(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_3,
//                XA_PLUS_RESOURCE_1);
//        // Test xa resources
//        TestXAResources xaResources = new TestXAResources();
//        // Initiate prepare recovery
//        dispatcher.dispatch(new XAPlusRecoveryPreparedEvent(xaResources.getJdbcConnections(),
//                xaResources.getJmsContexts(), xaResources.getXaResources(), dataSet.allPreparedXids,
//                dataSet.xaDanglingTransactions));
//        // Waiting retry requests
//        HashSet<String> superiors = new HashSet<>();
//        superiors.add(XA_PLUS_RESOURCE_2);
//        superiors.add(XA_PLUS_RESOURCE_3);
//        for (int i = 0; i < superiors.size(); i++) {
//            XAPlusRetryFromSuperiorRequestEvent retryFromSuperiorRequestEvent = retryFromSuperiorRequestEvents
//                    .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//            assertNotNull(retryFromSuperiorRequestEvent);
//            assertTrue(superiors.contains(retryFromSuperiorRequestEvent.getServerId()));
//        }
//    }

    private class ConsumerStub extends Bolt implements
            XAPlusRetryCommitOrderRequestEvent.Handler,
            XAPlusRetryRollbackOrderRequestEvent.Handler,
            XAPlusCommitRecoveredXidRequestEvent.Handler,
            XAPlusRollbackRecoveredXidRequestEvent.Handler,
            XAPlusRetryFromSuperiorRequestEvent.Handler {

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handleRetryCommitOrderRequest(XAPlusRetryCommitOrderRequestEvent event) throws InterruptedException {
            retryCommitOrderRequestEvents.put(event);
        }

        @Override
        public void handleCommitRecoveredXidRequest(XAPlusCommitRecoveredXidRequestEvent event) throws InterruptedException {
            commitRecoveredXidRequestEvents.add(event);
        }

        @Override
        public void handleRollbackRecoveredXidRequest(XAPlusRollbackRecoveredXidRequestEvent event) throws InterruptedException {
            rollbackRecoveredXidRequestEvents.add(event);
        }

        @Override
        public void handleRetryRollbackOrderRequest(XAPlusRetryRollbackOrderRequestEvent event) throws InterruptedException {
            retryRollbackOrderRequestEvents.put(event);
        }

        @Override
        public void handleRetryFromSuperiorRequest(XAPlusRetryFromSuperiorRequestEvent event) throws InterruptedException {
            retryFromSuperiorRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusRetryCommitOrderRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRetryRollbackOrderRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveryResourceRequestEvent.class);
            dispatcher.subscribe(this, XAPlusCommitRecoveredXidRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRecoveredXidRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRetryFromSuperiorRequestEvent.class);
        }
    }
}
