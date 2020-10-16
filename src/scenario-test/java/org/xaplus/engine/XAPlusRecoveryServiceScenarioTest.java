package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;
import org.xaplus.engine.events.journal.XAPlusLogCommitRecoveredXidDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackRecoveredXidDecisionEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateDoneEvent;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusRecoveryServiceScenarioTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryServiceScenarioTest.class);

    XAPlusRecoveryService xaPlusPreparerService;

    BlockingQueue<XAPlusFindDanglingTransactionsRequestEvent> findDanglingTransactionsRequestEvents;
    BlockingQueue<XAPlusRecoveryResourceRequestEvent> recoveryResourceRequestEvents;
    BlockingQueue<XAPlusDanglingTransactionCommittedEvent> danglingTransactionCommittedEvents;
    BlockingQueue<XAPlusDanglingTransactionRolledBackEvent> danglingTransactionRolledBackEvents;
    BlockingQueue<XAPlusLogCommitRecoveredXidDecisionEvent> logCommitRecoveredXidDecisionEvents;
    BlockingQueue<XAPlusLogRollbackRecoveredXidDecisionEvent> logRollbackRecoveredXidDecisionEvents;
    BlockingQueue<XAPlusCommitRecoveredXidRequestEvent> commitRecoveredXidRequestEvents;
    BlockingQueue<XAPlusRollbackRecoveredXidRequestEvent> rollbackRecoveredXidRequestEvents;
    BlockingQueue<XAPlusRetryCommitOrderRequestEvent> retryCommitOrderRequestEvents;
    BlockingQueue<XAPlusRetryRollbackOrderRequestEvent> retryRollbackOrderRequestEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);

        xaPlusPreparerService = new XAPlusRecoveryService(properties, threadPool, dispatcher, resources);
        xaPlusPreparerService.postConstruct();

        findDanglingTransactionsRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        recoveryResourceRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        danglingTransactionCommittedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        danglingTransactionRolledBackEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        logCommitRecoveredXidDecisionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        logRollbackRecoveredXidDecisionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitRecoveredXidRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRecoveredXidRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        retryCommitOrderRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        retryRollbackOrderRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusPreparerService.finish();
        consumerStub.finish();
    }

    @Test
    public void testSuperiorRecoveryScenario() throws InterruptedException {
        // Test data set
        TestSuperiorDataSet dataSet = new TestSuperiorDataSet();
        // Initiate server recovery
        dispatcher.dispatch(new XAPlusRecoveryRequestEvent());
        // Waiting find dangling transaction request
        XAPlusFindDanglingTransactionsRequestEvent findDanglingTransactionsRequestEvent =
                findDanglingTransactionsRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(findDanglingTransactionsRequestEvent);
        dispatcher.dispatch(new XAPlusDanglingTransactionsFoundEvent(dataSet.allDanglingTransactions));
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
        // Waiting retry orders requests for XA+ resources
        Set<XAPlusXid> orders = new HashSet<>();
        dataSet.xaPlusDanglingXids.keySet().forEach((xid) -> orders.add(xid));
        for (Map.Entry<XAPlusXid, Boolean> entry : dataSet.xaPlusDanglingXids.entrySet()) {
            Boolean status = entry.getValue();
            if (status) {
                XAPlusRetryCommitOrderRequestEvent retryCommitOrderRequestEvent =
                        retryCommitOrderRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
                assertNotNull(retryCommitOrderRequestEvent);
                assertTrue(orders.remove(retryCommitOrderRequestEvent.getXid()));
                // Response done
                dispatcher.dispatch(new XAPlusRemoteSubordinateDoneEvent(retryCommitOrderRequestEvent.getXid()));
            } else {
                XAPlusRetryRollbackOrderRequestEvent retryRollbackOrderRequestEvent =
                        retryRollbackOrderRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
                assertNotNull(retryRollbackOrderRequestEvent);
                assertTrue(orders.remove(retryRollbackOrderRequestEvent.getXid()));
                // Response done
                dispatcher.dispatch(new XAPlusRemoteSubordinateDoneEvent(retryRollbackOrderRequestEvent.getXid()));
            }
        }
        assertTrue(orders.isEmpty());
        // Waiting commit or rollback requests for recovered xids
        for (Map.Entry<XAPlusXid, Boolean> entry : dataSet.xaDanglingXids.entrySet()) {
            Boolean status = entry.getValue();
            if (status) {
                XAPlusCommitRecoveredXidRequestEvent commitRecoveredXidRequestEvent =
                        commitRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
                assertNotNull(commitRecoveredXidRequestEvent);
                assertTrue(dataSet.xaDanglingXids.containsKey(commitRecoveredXidRequestEvent.getXid()));
            } else {
                XAPlusRollbackRecoveredXidRequestEvent rollbackRecoveredXidRequestEvent =
                        rollbackRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
                XAPlusXid xid = rollbackRecoveredXidRequestEvent.getXid();
                if (!dataSet.xaDanglingXids.containsKey(xid)) {
                    assertTrue(dataSet.allPreparedXids
                            .get(rollbackRecoveredXidRequestEvent.getUniqueName()).contains(xid));
                }
            }
        }
    }

    private class ConsumerStub extends Bolt implements
            XAPlusFindDanglingTransactionsRequestEvent.Handler,
            XAPlusRecoveryResourceRequestEvent.Handler,
            XAPlusDanglingTransactionCommittedEvent.Handler,
            XAPlusDanglingTransactionRolledBackEvent.Handler,
            XAPlusLogCommitRecoveredXidDecisionEvent.Handler,
            XAPlusLogRollbackRecoveredXidDecisionEvent.Handler,
            XAPlusCommitRecoveredXidRequestEvent.Handler,
            XAPlusRollbackRecoveredXidRequestEvent.Handler,
            XAPlusRetryCommitOrderRequestEvent.Handler,
            XAPlusRetryRollbackOrderRequestEvent.Handler {

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handleCommitRecoveredXidRequest(XAPlusCommitRecoveredXidRequestEvent event) throws InterruptedException {
            commitRecoveredXidRequestEvents.add(event);
        }

        @Override
        public void handleDanglingTransactionCommitted(XAPlusDanglingTransactionCommittedEvent event) throws InterruptedException {
            danglingTransactionCommittedEvents.add(event);
        }

        @Override
        public void handleDanglingTransactionRolledBack(XAPlusDanglingTransactionRolledBackEvent event) throws InterruptedException {
            danglingTransactionRolledBackEvents.add(event);
        }

        @Override
        public void handleFindDanglingTransactionsRequest(XAPlusFindDanglingTransactionsRequestEvent event) throws InterruptedException {
            findDanglingTransactionsRequestEvents.add(event);
        }

        @Override
        public void handleRecoveryResourceRequest(XAPlusRecoveryResourceRequestEvent event) throws InterruptedException {
            recoveryResourceRequestEvents.add(event);
        }

        @Override
        public void handleRollbackRecoveredXidRequest(XAPlusRollbackRecoveredXidRequestEvent event) throws InterruptedException {
            rollbackRecoveredXidRequestEvents.add(event);
        }

        @Override
        public void handleLogCommitRecoveredXidDecision(XAPlusLogCommitRecoveredXidDecisionEvent event) throws InterruptedException {
            logCommitRecoveredXidDecisionEvents.add(event);
        }

        @Override
        public void handleLogRollbackRecoveredXidDecision(XAPlusLogRollbackRecoveredXidDecisionEvent event) throws InterruptedException {
            logRollbackRecoveredXidDecisionEvents.add(event);
        }

        @Override
        public void handleRetryCommitOrderRequest(XAPlusRetryCommitOrderRequestEvent event) throws InterruptedException {
            retryCommitOrderRequestEvents.put(event);
        }

        @Override
        public void handleRetryRollbackOrderRequest(XAPlusRetryRollbackOrderRequestEvent event) throws InterruptedException {
            retryRollbackOrderRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusFindDanglingTransactionsRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveryResourceRequestEvent.class);
            dispatcher.subscribe(this, XAPlusDanglingTransactionCommittedEvent.class);
            dispatcher.subscribe(this, XAPlusDanglingTransactionRolledBackEvent.class);
            dispatcher.subscribe(this, XAPlusLogCommitRecoveredXidDecisionEvent.class);
            dispatcher.subscribe(this, XAPlusLogRollbackRecoveredXidDecisionEvent.class);
            dispatcher.subscribe(this, XAPlusCommitRecoveredXidRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRecoveredXidRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRetryCommitOrderRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRetryRollbackOrderRequestEvent.class);
        }
    }

    private class TestSuperiorDataSet {

        final TestSuperiorTransaction transaction1;
        final TestSuperiorTransaction transaction2;
        final TestSuperiorTransaction transaction3;

        final Set<XAPlusXid> xaResource1PreparedXids;
        final Set<XAPlusXid> xaResource2PreparedXids;
        final Set<XAPlusXid> xaResource3PreparedXids;

        final Map<String, Set<XAPlusXid>> allPreparedXids;

        final Map<String, Map<XAPlusXid, Boolean>> xaDanglingTransactions;
        final Map<String, Map<XAPlusXid, Boolean>> xaPlusDanglingTransactions;
        final Map<String, Map<XAPlusXid, Boolean>> allDanglingTransactions;

        final Map<XAPlusXid, Boolean> xaDanglingXids;
        final Map<XAPlusXid, Boolean> xaPlusDanglingXids;

        TestSuperiorDataSet() {
            transaction1 = new TestSuperiorTransaction();
            transaction2 = new TestSuperiorTransaction();
            transaction3 = new TestSuperiorTransaction();

            xaResource1PreparedXids = new HashSet<>();
            xaResource1PreparedXids.addAll(transaction1.xaResource1PreparedXids);
            xaResource1PreparedXids.addAll(transaction2.xaResource1PreparedXids);
            xaResource1PreparedXids.addAll(transaction3.xaResource1PreparedXids);

            xaResource2PreparedXids = new HashSet<>();
            xaResource2PreparedXids.addAll(transaction1.xaResource2PreparedXids);
            xaResource2PreparedXids.addAll(transaction2.xaResource2PreparedXids);
            xaResource2PreparedXids.addAll(transaction3.xaResource2PreparedXids);

            xaResource3PreparedXids = new HashSet<>();
            xaResource3PreparedXids.addAll(transaction1.xaResource3PreparedXids);
            xaResource3PreparedXids.addAll(transaction2.xaResource3PreparedXids);
            xaResource3PreparedXids.addAll(transaction3.xaResource3PreparedXids);

            allPreparedXids = new HashMap<>();
            allPreparedXids.put(XA_RESOURCE_1, xaResource1PreparedXids);
            allPreparedXids.put(XA_RESOURCE_2, xaResource2PreparedXids);
            allPreparedXids.put(XA_RESOURCE_3, xaResource3PreparedXids);

            Map<XAPlusXid, Boolean> xaResource1AllDanglingXids = new HashMap<>();
            xaResource1AllDanglingXids.putAll(transaction1.xaResource1DanglingXids);
            xaResource1AllDanglingXids.putAll(transaction2.xaResource1DanglingXids);
            xaResource1AllDanglingXids.putAll(transaction3.xaResource1DanglingXids);

            Map<XAPlusXid, Boolean> xaResource2AllDanglingXids = new HashMap<>();
            xaResource2AllDanglingXids.putAll(transaction1.xaResource2DanglingXids);
            xaResource2AllDanglingXids.putAll(transaction2.xaResource2DanglingXids);
            xaResource2AllDanglingXids.putAll(transaction3.xaResource2DanglingXids);

            Map<XAPlusXid, Boolean> xaResource3AllDanglingXids = new HashMap<>();
            xaResource3AllDanglingXids.putAll(transaction1.xaResource3DanglingXids);
            xaResource3AllDanglingXids.putAll(transaction2.xaResource3DanglingXids);
            xaResource3AllDanglingXids.putAll(transaction3.xaResource3DanglingXids);

            Map<XAPlusXid, Boolean> xaPlusResource2AllDanglingXids = new HashMap<>();
            xaPlusResource2AllDanglingXids.putAll(transaction1.xaPlusResource2DanglingXids);
            xaPlusResource2AllDanglingXids.putAll(transaction2.xaPlusResource2DanglingXids);

            Map<XAPlusXid, Boolean> xaPlusResource3AllDanglingXids = new HashMap<>();
            xaPlusResource3AllDanglingXids.putAll(transaction1.xaPlusResource3DanglingXids);
            xaPlusResource3AllDanglingXids.putAll(transaction2.xaPlusResource3DanglingXids);

            xaDanglingTransactions = new HashMap<>();
            xaDanglingTransactions.put(XA_RESOURCE_1, xaResource1AllDanglingXids);
            xaDanglingTransactions.put(XA_RESOURCE_2, xaResource2AllDanglingXids);
            xaDanglingTransactions.put(XA_RESOURCE_3, xaResource3AllDanglingXids);

            xaPlusDanglingTransactions = new HashMap<>();
            xaPlusDanglingTransactions.put(XA_PLUS_RESOURCE_2, xaPlusResource2AllDanglingXids);
            xaPlusDanglingTransactions.put(XA_PLUS_RESOURCE_3, xaPlusResource3AllDanglingXids);

            allDanglingTransactions = new HashMap<>();
            allDanglingTransactions.putAll(xaDanglingTransactions);
            allDanglingTransactions.putAll(xaPlusDanglingTransactions);

            xaDanglingXids = new HashMap<>();
            xaDanglingXids.putAll(xaResource1AllDanglingXids);
            xaDanglingXids.putAll(xaResource2AllDanglingXids);
            xaDanglingXids.putAll(xaResource3AllDanglingXids);

            xaPlusDanglingXids = new HashMap<>();
            xaPlusDanglingXids.putAll(xaPlusResource2AllDanglingXids);
            xaPlusDanglingXids.putAll(xaPlusResource3AllDanglingXids);
        }
    }

    private class TestSuperiorTransaction extends TestTransaction {

        TestSuperiorTransaction() {
            super(createSuperiorTransaction());
        }
    }

    private class TestTransaction {

        final XAPlusTransaction transaction;

        final Set<XAPlusXid> xaResource1PreparedXids;
        final Map<XAPlusXid, Boolean> xaResource1DanglingXids;
        final Set<XAPlusXid> xaResource2PreparedXids;
        final Map<XAPlusXid, Boolean> xaResource2DanglingXids;
        final Set<XAPlusXid> xaResource3PreparedXids;
        final Map<XAPlusXid, Boolean> xaResource3DanglingXids;

        final Map<XAPlusXid, Boolean> xaPlusResource2DanglingXids;
        final Map<XAPlusXid, Boolean> xaPlusResource3DanglingXids;

        TestTransaction(XAPlusTransaction transaction) {
            this.transaction = transaction;

            xaResource1PreparedXids = new HashSet<>();
            xaResource1DanglingXids = new HashMap<>();
            XAPlusXid xid11 = createJdbcXid(transaction);
            xaResource1PreparedXids.add(xid11);
            // xid11 - only prepared, not commited or rolledback
            // xaResource1DanglingXids.put(xid11, true);
            XAPlusXid xid12 = createJdbcXid(transaction);
            xaResource1PreparedXids.add(xid12);
            xaResource1DanglingXids.put(xid12, true);
            XAPlusXid xid13 = createJdbcXid(transaction);
            xaResource1PreparedXids.add(xid13);
            xaResource1DanglingXids.put(xid13, false);

            xaResource2PreparedXids = new HashSet<>();
            xaResource2DanglingXids = new HashMap<>();
            XAPlusXid xid21 = createJdbcXid(transaction);
            xaResource2PreparedXids.add(xid21);
            xaResource2DanglingXids.put(xid21, true);
            XAPlusXid xid22 = createJdbcXid(transaction);
            xaResource2PreparedXids.add(xid22);
            // xid12 - only prepared, not commited or rolledback
            // xaResource2DanglingXids.put(xid22, true);
            XAPlusXid xid23 = createJdbcXid(transaction);
            xaResource2PreparedXids.add(xid23);
            xaResource2DanglingXids.put(xid23, false);

            xaResource3PreparedXids = new HashSet<>();
            xaResource3DanglingXids = new HashMap<>();
            XAPlusXid xid31 = createJdbcXid(transaction);
            xaResource3PreparedXids.add(xid31);
            xaResource3DanglingXids.put(xid31, true);
            XAPlusXid xid32 = createJdbcXid(transaction);
            xaResource3PreparedXids.add(xid32);
            xaResource3DanglingXids.put(xid32, true);
            XAPlusXid xid33 = createJdbcXid(transaction);
            xaResource3PreparedXids.add(xid33);
            // xid33 - only prepared, not commited or rolledback
            // xaResource3DanglingXids.put(xid33, true);

            xaPlusResource2DanglingXids = new HashMap<>();
            xaPlusResource2DanglingXids.put(createXAPlusXid(transaction, XA_PLUS_RESOURCE_2), true);
            xaPlusResource2DanglingXids.put(createXAPlusXid(transaction, XA_PLUS_RESOURCE_2), false);

            xaPlusResource3DanglingXids = new HashMap<>();
            xaPlusResource3DanglingXids.put(createXAPlusXid(transaction, XA_PLUS_RESOURCE_3), true);
            xaPlusResource3DanglingXids.put(createXAPlusXid(transaction, XA_PLUS_RESOURCE_3), false);
        }
    }
}
