package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlusCommitTransactionDecisionEvent;
import org.xaplus.engine.events.twopc.XAPlusRollbackTransactionDecisionEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCreateTransactionEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xa.XAPlusBranchPreparedEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchFailedEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchRequestEvent;
import org.xaplus.engine.events.xaplus.*;
import org.xaplus.engine.stubs.XAConnectionStub;

import javax.transaction.xa.XAException;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusSubordinatePreparerServiceUnitTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSubordinatePreparerServiceUnitTest.class);

    private XAPlusSubordinatePreparerService xaPlusSubordinatePreparerService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        xaPlusSubordinatePreparerService =
                new XAPlusSubordinatePreparerService(properties, threadPool, dispatcher, resources);
        xaPlusSubordinatePreparerService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusSubordinatePreparerService.finish();
        consumerStub.finish();
    }

    @Test
    public void testBranchSuccessfulAndCommitOrder() throws InterruptedException, SQLException, XAException {
        // XA_PLUS_RESOURCE_1 plays subordinate role
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createJdbcXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAConnectionStub());
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAConnectionStub());
        // Create
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // User commit
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        // Prepare 1
        XAPlusPrepareBranchRequestEvent event1 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction.getXid(), event1.getXid());
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(event1.getXid(), event1.getBranchXid()));
        // Prepare 2
        XAPlusPrepareBranchRequestEvent event2 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction.getXid(), event2.getXid());
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(event2.getXid(), event2.getBranchXid()));
        // Wait ready status
        XAPlusReportReadyStatusRequestEvent event3 = consumerStub.reportReadyStatusRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event3);
        assertEquals(transaction.getXid(), event3.getXid());
        // Commit order from superior and wait commit decision
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToCommitEvent(event3.getXid()));
        XAPlusCommitTransactionDecisionEvent event4 = consumerStub.commitTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event4);
        assertEquals(transaction, event4.getTransaction());
    }

    @Test
    public void testBranchFailedAndRollbackOrder() throws InterruptedException, SQLException, XAException {
        // XA_PLUS_RESOURCE_1 plays subordinate role
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createJdbcXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAConnectionStub());
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAConnectionStub());
        // Create
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // User commit
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        // Prepare 1
        XAPlusPrepareBranchRequestEvent event1 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction.getXid(), event1.getXid());
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(event1.getXid(), event1.getBranchXid()));
        // Prepare 2 - failed
        XAPlusPrepareBranchRequestEvent event2 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction.getXid(), event2.getXid());
        dispatcher.dispatch(new XAPlusPrepareBranchFailedEvent(event2.getXid(), event2.getBranchXid(),
                new Exception("prepare_exception")));
        // Wait failed status
        XAPlusReportFailedStatusRequestEvent event3 = consumerStub.reportFailedStatusRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event3);
        assertEquals(transaction.getXid(), event3.getXid());
        // Rollback order from superior and wait rollback decision
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(event3.getXid()));
        XAPlusRollbackTransactionDecisionEvent event4 = consumerStub.rollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event4);
        assertEquals(transaction, event4.getTransaction());
    }

    @Test
    public void testBranchSuccessfulAndRollbackOrder()
            throws InterruptedException, SQLException, XAException {
        // XA_PLUS_RESOURCE_1 plays subordinate role
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createJdbcXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAConnectionStub());
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAConnectionStub());
        // Create
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // User commit
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        // Prepare 1
        XAPlusPrepareBranchRequestEvent event1 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction.getXid(), event1.getXid());
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(event1.getXid(), event1.getBranchXid()));
        // Prepare 2
        XAPlusPrepareBranchRequestEvent event2 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction.getXid(), event2.getXid());
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(event2.getXid(), event2.getBranchXid()));
        // Wait ready status
        XAPlusReportReadyStatusRequestEvent event3 = consumerStub.reportReadyStatusRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event3);
        assertEquals(transaction.getXid(), event3.getXid());
        // Rollback order from superior and wait rollback decision
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(event3.getXid()));
        XAPlusRollbackTransactionDecisionEvent event4 = consumerStub.rollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event4);
        assertEquals(transaction, event4.getTransaction());
    }

    @Test
    public void testUserRollbackBeforeOrder()
            throws InterruptedException, SQLException, XAException {
        // XA_PLUS_RESOURCE_1 plays subordinate role
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createJdbcXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAConnectionStub());
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAConnectionStub());
        // Create
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // User rollback
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        // Wait failed status
        XAPlusReportFailedStatusRequestEvent event1 = consumerStub.reportFailedStatusRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction.getXid(), event1.getXid());
        // Rollback order from superior and wait rollback decision
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(event1.getXid()));
        XAPlusRollbackTransactionDecisionEvent event2 = consumerStub.rollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction, event2.getTransaction());
    }

    @Test
    public void testRollbackOrderBeforeUserDecision()
            throws InterruptedException, SQLException, XAException {
        // XA_PLUS_RESOURCE_1 plays subordinate role
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createJdbcXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAConnectionStub());
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAConnectionStub());
        // Create
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // Rollback order
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(transaction.getXid()));
        // User rollback and wait rollback decision
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        XAPlusRollbackTransactionDecisionEvent event1 = consumerStub.rollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction, event1.getTransaction());
    }

    @Test
    public void testSuperiorRollbackOrderBeforeUserCommit() throws InterruptedException, SQLException, XAException {
        // XA_PLUS_RESOURCE_1 plays subordinate role
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createJdbcXid(transaction);
        transaction.enlist(bxid1, XA_RESOURCE_1, new XAConnectionStub());
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_2, new XAConnectionStub());
        // Create
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // Superior rollback order
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(transaction.getXid()));
        // User commit
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        // Prepare 1
        XAPlusPrepareBranchRequestEvent event1 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction.getXid(), event1.getXid());
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(event1.getXid(), event1.getBranchXid()));
        // Prepare 2
        XAPlusPrepareBranchRequestEvent event2 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction.getXid(), event2.getXid());
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(event2.getXid(), event2.getBranchXid()));
        // Wait rollback decision
        XAPlusRollbackTransactionDecisionEvent event3 = consumerStub.rollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event3);
        assertEquals(transaction, event3.getTransaction());
    }

    @Test
    public void testReportReadyStatusFailed() throws InterruptedException, SQLException, XAException {
        // XA_PLUS_RESOURCE_1 plays subordinate role
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_1);
        // Create
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // Report ready failed, wait 2pc failed event
        dispatcher.dispatch(new XAPlusReportReadyStatusFailedEvent(transaction.getXid(),
                new Exception("ready_exception")));
        XAPlus2pcFailedEvent event1 = consumerStub.twoPcFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction, event1.getTransaction());
    }

    @Test
    public void testReportFailedStatusFailed() throws InterruptedException, SQLException, XAException {
        // XA_PLUS_RESOURCE_1 plays subordinate role
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_2, XA_PLUS_RESOURCE_1);
        // Create
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        // Report failed failed, wait 2pc failed event
        dispatcher.dispatch(new XAPlusReportFailedStatusFailedEvent(transaction.getXid(),
                new Exception("failed_exception")));
        XAPlus2pcFailedEvent event1 = consumerStub.twoPcFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction, event1.getTransaction());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusPrepareBranchRequestEvent.Handler,
            XAPlusReportReadyStatusRequestEvent.Handler,
            XAPlusReportFailedStatusRequestEvent.Handler,
            XAPlusCommitTransactionDecisionEvent.Handler,
            XAPlusRollbackTransactionDecisionEvent.Handler,
            XAPlus2pcFailedEvent.Handler {

        BlockingQueue<XAPlusPrepareBranchRequestEvent> prepareBranchRequestEvents;
        BlockingQueue<XAPlusReportReadyStatusRequestEvent> reportReadyStatusRequestEvents;
        BlockingQueue<XAPlusReportFailedStatusRequestEvent> reportFailedStatusRequestEvents;
        BlockingQueue<XAPlusCommitTransactionDecisionEvent> commitTransactionDecisionEvents;
        BlockingQueue<XAPlusRollbackTransactionDecisionEvent> rollbackTransactionDecisionEvents;
        BlockingQueue<XAPlus2pcFailedEvent> twoPcFailedEvents;

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
            prepareBranchRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            reportReadyStatusRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            reportFailedStatusRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            commitTransactionDecisionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackTransactionDecisionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            twoPcFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handlePrepareBranchRequest(XAPlusPrepareBranchRequestEvent event) throws InterruptedException {
            prepareBranchRequestEvents.put(event);
        }

        @Override
        public void handleReportReadyStatusRequest(XAPlusReportReadyStatusRequestEvent event)
                throws InterruptedException {
            reportReadyStatusRequestEvents.put(event);
        }

        @Override
        public void handleReportFailedStatusRequest(XAPlusReportFailedStatusRequestEvent event)
                throws InterruptedException {
            reportFailedStatusRequestEvents.put(event);
        }

        @Override
        public void handleCommitTransactionDecision(XAPlusCommitTransactionDecisionEvent event)
                throws InterruptedException {
            commitTransactionDecisionEvents.put(event);
        }

        @Override
        public void handleRollbackTransactionDecision(XAPlusRollbackTransactionDecisionEvent event)
                throws InterruptedException {
            rollbackTransactionDecisionEvents.put(event);
        }

        @Override
        public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
            twoPcFailedEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusPrepareBranchRequestEvent.class);
            dispatcher.subscribe(this, XAPlusReportReadyStatusRequestEvent.class);
            dispatcher.subscribe(this, XAPlusReportFailedStatusRequestEvent.class);
            dispatcher.subscribe(this, XAPlusCommitTransactionDecisionEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionEvent.class);
            dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        }
    }
}
