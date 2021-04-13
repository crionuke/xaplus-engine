package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackTransactionDecisionEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCreateTransactionEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.events.xa.XAPlusBranchPreparedEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchFailedEvent;
import org.xaplus.engine.events.xa.XAPlusPrepareBranchRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateReadyEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;
import org.xaplus.engine.stubs.XAConnectionStub;

import javax.transaction.xa.XAException;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusSuperiorPreparerServiceUnitTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusSuperiorPreparerServiceUnitTest.class);

    private XAPlusSuperiorPreparerService xaPlusSuperiorPreparerService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        xaPlusSuperiorPreparerService = new XAPlusSuperiorPreparerService(properties, threadPool, dispatcher);
        xaPlusSuperiorPreparerService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusSuperiorPreparerService.finish();
        consumerStub.finish();
    }

    @Test
    public void testUserRollbackRequest() throws InterruptedException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        XAPlusLogRollbackTransactionDecisionEvent event = consumerStub.logRollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void testCommitAfterSubordinateWithOnlyXAPlusBranches() throws InterruptedException, XAPlusSystemException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_2);
        transaction.enlist(bxid1, XA_PLUS_RESOURCE_2, resources.getXAPlusResource(XA_PLUS_RESOURCE_2));
        XAPlusXid bxid2 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_3);
        transaction.enlist(bxid2, XA_PLUS_RESOURCE_3, resources.getXAPlusResource(XA_PLUS_RESOURCE_3));
        // Subordinates report before user commit
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusRemoteSubordinateReadyEvent(bxid1));
        dispatcher.dispatch(new XAPlusRemoteSubordinateFailedEvent(bxid2));
        // No decision as no commit order
        XAPlusLogRollbackTransactionDecisionEvent event1 = consumerStub.logRollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNull(event1);
        // Send commit order
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        // Rollback as transaction has failures
        XAPlusLogRollbackTransactionDecisionEvent event2 = consumerStub.logRollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction, event2.getTransaction());
    }

    @Test
    public void testCommitBeforeSubordinateWithOnlyXAPlusBranches()
            throws InterruptedException, XAPlusSystemException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_2);
        transaction.enlist(bxid1, XA_PLUS_RESOURCE_2, resources.getXAPlusResource(XA_PLUS_RESOURCE_2));
        XAPlusXid bxid2 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_3);
        transaction.enlist(bxid2, XA_PLUS_RESOURCE_3, resources.getXAPlusResource(XA_PLUS_RESOURCE_3));
        // User commit before subordinates report
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRemoteSubordinateReadyEvent(bxid1));
        dispatcher.dispatch(new XAPlusRemoteSubordinateReadyEvent(bxid2));
        // As no failures
        XAPlusLogCommitTransactionDecisionEvent event2 = consumerStub.logCommitTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction, event2.getTransaction());
    }

    @Test
    public void testCommitSuccessful() throws InterruptedException, XAPlusSystemException, SQLException, XAException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_2);
        transaction.enlist(bxid1, XA_PLUS_RESOURCE_2, resources.getXAPlusResource(XA_PLUS_RESOURCE_2));
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_1, new XAConnectionStub());
        // Create, user commit order, subordinate report
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRemoteSubordinateReadyEvent(bxid1));
        // Prepare request
        XAPlusPrepareBranchRequestEvent event1 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction.getXid(), event1.getXid());
        assertEquals(bxid2, event1.getBranchXid());
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(transaction.getXid(), bxid2));
        // As no failures
        XAPlusLogCommitTransactionDecisionEvent event2 = consumerStub.logCommitTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(transaction, event2.getTransaction());
    }

    @Test
    public void testCommitFailed() throws InterruptedException, XAPlusSystemException, SQLException, XAException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        XAPlusXid bxid1 = createXAPlusXid(transaction, XA_PLUS_RESOURCE_2);
        transaction.enlist(bxid1, XA_PLUS_RESOURCE_2, resources.getXAPlusResource(XA_PLUS_RESOURCE_2));
        XAPlusXid bxid2 = createJdbcXid(transaction);
        transaction.enlist(bxid2, XA_RESOURCE_1, new XAConnectionStub());
        // Create, user commit order, subordinate report
        dispatcher.dispatch(new XAPlusUserCreateTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRemoteSubordinateFailedEvent(bxid1));
        // Prepare request
        XAPlusPrepareBranchRequestEvent event1 = consumerStub.prepareBranchRequestEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(transaction.getXid(), event1.getXid());
        assertEquals(bxid2, event1.getBranchXid());
        // No rollback as waiting preparation
        XAPlusLogRollbackTransactionDecisionEvent event2 = consumerStub.logRollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNull(event2);
        // Branch failed
        dispatcher.dispatch(new XAPlusPrepareBranchFailedEvent(transaction.getXid(), bxid2,
                new Exception("prepare_exception")));
        // As transaction has failures
        XAPlusLogRollbackTransactionDecisionEvent event3 = consumerStub.logRollbackTransactionDecisionEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event3);
        assertEquals(transaction, event3.getTransaction());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusLogRollbackTransactionDecisionEvent.Handler,
            XAPlusLogCommitTransactionDecisionEvent.Handler,
            XAPlusPrepareBranchRequestEvent.Handler {

        BlockingQueue<XAPlusLogRollbackTransactionDecisionEvent> logRollbackTransactionDecisionEvents;
        BlockingQueue<XAPlusLogCommitTransactionDecisionEvent> logCommitTransactionDecisionEvents;
        BlockingQueue<XAPlusPrepareBranchRequestEvent> prepareBranchRequestEvents;

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
            logRollbackTransactionDecisionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            logCommitTransactionDecisionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            prepareBranchRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleLogRollbackTransactionDecision(XAPlusLogRollbackTransactionDecisionEvent event) throws InterruptedException {
            logRollbackTransactionDecisionEvents.put(event);
        }

        @Override
        public void handleLogCommitTransactionDecision(XAPlusLogCommitTransactionDecisionEvent event) throws InterruptedException {
            logCommitTransactionDecisionEvents.put(event);
        }

        @Override
        public void handlePrepareBranchRequest(XAPlusPrepareBranchRequestEvent event) throws InterruptedException {
            prepareBranchRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionEvent.class);
            dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionEvent.class);
            dispatcher.subscribe(this, XAPlusPrepareBranchRequestEvent.class);
        }
    }
}
