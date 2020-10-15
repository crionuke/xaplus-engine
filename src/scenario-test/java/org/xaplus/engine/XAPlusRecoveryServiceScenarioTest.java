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

import java.util.HashMap;
import java.util.Map;
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
        createXAPlusComponents(SERVER_ID_DEFAULT);

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
    public void testRecoveryScenario() throws InterruptedException {
        // Test branches
        XAPlusTransaction transaction = createSuperiorTransaction();
        Map<XAPlusXid, Boolean> xaResource1Xids = new HashMap();
        xaResource1Xids.put(enlistJdbc(transaction), true);
        Map<XAPlusXid, Boolean> xaResource2Xids = new HashMap<>();
        xaResource2Xids.put(enlistJdbc(transaction), false);
        xaResource2Xids.put(enlistJdbc(transaction), true);
        Map<XAPlusXid, Boolean> xaResource3Xids = new HashMap<>();
        xaResource3Xids.put(enlistJdbc(transaction), true);
        xaResource3Xids.put(enlistJdbc(transaction), false);
        xaResource3Xids.put(enlistJdbc(transaction), true);
        Map<XAPlusXid, Boolean> xaPlusResource1Xids = new HashMap();
        xaPlusResource1Xids.put(enlistXAPlus(transaction, SERVER_ID_2), true);
        xaPlusResource1Xids.put(enlistXAPlus(transaction, SERVER_ID_2), false);
        Map<XAPlusXid, Boolean> xaPlusResource2Xids = new HashMap();
        xaPlusResource2Xids.put(enlistXAPlus(transaction, SERVER_ID_3), true);
        xaPlusResource2Xids.put(enlistXAPlus(transaction, SERVER_ID_3), false);
        // Dangling transactions
        Map<String, Map<XAPlusXid, Boolean>> transactions = new HashMap<>();
        transactions.put(XA_RESOURCE_1, xaResource1Xids);
        transactions.put(XA_RESOURCE_2, xaResource2Xids);
        transactions.put(XA_RESOURCE_3, xaResource3Xids);
        transactions.put(XA_PLUS_RESOURCE_1, xaPlusResource1Xids);
        transactions.put(XA_PLUS_RESOURCE_2, xaPlusResource2Xids);
        // Initiate server recovery
        dispatcher.dispatch(new XAPlusRecoveryRequestEvent());
        // Waiting find dangling transaction request
        XAPlusFindDanglingTransactionsRequestEvent findDanglingTransactionsRequestEvent =
                findDanglingTransactionsRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(findDanglingTransactionsRequestEvent);
        dispatcher.dispatch(new XAPlusDanglingTransactionsFoundEvent(transactions));
        // Waiting recovery requests for all XA resources
        for (int i = 0; i < 3; i++) {
            XAPlusRecoveryResourceRequestEvent recoveryResourceRequestEvent =
                    recoveryResourceRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            assertNotNull(recoveryResourceRequestEvent);
            String uniqueName = recoveryResourceRequestEvent.getUniqueName();
            if (transactions.containsKey(uniqueName)) {
                // Response
                dispatcher.dispatch(
                        new XAPlusResourceRecoveredEvent(uniqueName, transactions.get(uniqueName).keySet()));
            } else {
                fail("unknown resource name " + uniqueName);
            }
        }
        // Waiting retry requests
        Map<XAPlusXid, Boolean> xaPlusXids = new HashMap();
        xaPlusXids.putAll(xaPlusResource1Xids);
        xaPlusXids.putAll(xaPlusResource2Xids);
        for (Map.Entry<XAPlusXid, Boolean> entry: xaPlusXids.entrySet()) {
            Boolean status = entry.getValue();
            if (status) {
                XAPlusRetryCommitOrderRequestEvent commitOrderRequestEvent =
                        retryCommitOrderRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
                assertNotNull(commitOrderRequestEvent);
                assertTrue(xaPlusXids.containsKey(commitOrderRequestEvent.getXid()));
            } else {
                XAPlusRetryRollbackOrderRequestEvent rollbackOrderRequestEvent =
                        retryRollbackOrderRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
                assertNotNull(rollbackOrderRequestEvent);
                assertTrue(xaPlusXids.containsKey(rollbackOrderRequestEvent.getXid()));
            }
        }
        //TODO: xa recovery testing
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
}
