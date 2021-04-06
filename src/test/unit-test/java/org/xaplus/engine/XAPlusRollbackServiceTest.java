package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusLogRollbackTransactionDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogRollbackTransactionDecisionFailedEvent;
import org.xaplus.engine.events.journal.XAPlusRollbackTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.rollback.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.xa.XAPlusRollbackBranchRequestEvent;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusRollbackServiceTest extends XAPlusTest {
public class XAPlusRollbackServiceTest extends XAPlusUnitTest {
//    static private final Logger logger = LoggerFactory.getLogger(XAPlusRollbackServiceTest.class);
//
//    XAPlusRollbackService xaPlusRollbackService;
//
//    BlockingQueue<XAPlusLogRollbackTransactionDecisionEvent> logRollbackTransactionDecisionEvents;
//    BlockingQueue<XAPlusRollbackBranchRequestEvent> rollbackBranchRequestEvents;
//    BlockingQueue<XAPlusRollbackFailedEvent> rollbackFailedEvents;
//    BlockingQueue<XAPlusRollbackDoneEvent> rollbackDoneEvents;
//
//    ConsumerStub consumerStub;
//
//    @Before
//    public void beforeTest() {
//        createXAPlusComponents(XA_PLUS_RESOURCE_1);
//
//        xaPlusRollbackService = new XAPlusRollbackService(properties, threadPool, dispatcher, new XAPlusTracker());
//        xaPlusRollbackService.postConstruct();
//
//        logRollbackTransactionDecisionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
//        rollbackBranchRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
//        rollbackFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
//        rollbackDoneEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
//
//        consumerStub = new ConsumerStub();
//        consumerStub.postConstruct();
//    }
//
//    @After
//    public void afterTest() {
//        xaPlusRollbackService.finish();
//        consumerStub.finish();
//    }
//
//    @Test
//    public void testRollbackRequest() throws InterruptedException {
//        XAPlusTransaction transaction = createTestSuperiorTransaction();
//        dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
//        XAPlusLogRollbackTransactionDecisionEvent event =
//                logRollbackTransactionDecisionEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event);
//        assertEquals(transaction, event.getTransaction());
//    }
//
//    @Test
//    public void testRollbackTransactionDecisionLogged() throws InterruptedException {
//        XAPlusTransaction transaction = createTestSuperiorTransaction();
//        dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
//        dispatcher.dispatch(new XAPlusRollbackTransactionDecisionLoggedEvent(transaction));
//        Set<XAPlusXid> branches = transaction.getAllXids();
//        while (!branches.isEmpty()) {
//            XAPlusRollbackBranchRequestEvent event =
//                    rollbackBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//            assertNotNull(event);
//            branches.remove(event.getBranchXid());
//        }
//        assertTrue(branches.isEmpty());
//    }
//
//    @Test
//    public void testLogRollbackTransactionDecisionFailed() throws InterruptedException {
//        XAPlusTransaction transaction = createTestSuperiorTransaction();
//        dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
//        dispatcher.dispatch(new XAPlusLogRollbackTransactionDecisionFailedEvent(transaction,
//                new Exception("log_exception")));
//        XAPlusRollbackFailedEvent event = rollbackFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event);
//        assertEquals(transaction, event.getTransaction());
//    }
//
//    @Test
//    public void testRollbackDone() throws InterruptedException {
//        XAPlusTransaction transaction = createTestSuperiorTransaction();
//        dispatcher.dispatch(new XAPlusRollbackRequestEvent(transaction));
////        for (XAPlusXid bxid : transaction.getXaResources().keySet()) {
////            dispatcher.dispatch(new XAPlusBranchRolledBackEvent(transaction.getXid(), bxid));
////        }
////        for (XAPlusXid bxid : transaction.getXaPlusResources().keySet()) {
////            dispatcher.dispatch(new XAPlusRemoteSubordinateDoneEvent(bxid));
////        }
////        XAPlusRollbackDoneEvent event = rollbackDoneEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
////        assertNotNull(event);
////        assertEquals(transaction, event.getTransaction());
//    }
//
//    private class ConsumerStub extends Bolt implements
//            XAPlusLogRollbackTransactionDecisionEvent.Handler,
//            XAPlusRollbackBranchRequestEvent.Handler,
//            XAPlusRollbackFailedEvent.Handler,
//            XAPlusRollbackDoneEvent.Handler {
//
//        ConsumerStub() {
//            super("stub-consumer", QUEUE_SIZE);
//        }
//
//        @Override
//        public void handleLogRollbackTransactionDecision(XAPlusLogRollbackTransactionDecisionEvent event) throws InterruptedException {
//            logRollbackTransactionDecisionEvents.put(event);
//        }
//
//        @Override
//        public void handleRollbackBranchRequest(XAPlusRollbackBranchRequestEvent event) throws InterruptedException {
//            rollbackBranchRequestEvents.put(event);
//        }
//
//        @Override
//        public void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException {
//            rollbackFailedEvents.put(event);
//        }
//
//        @Override
//        public void handleRollbackDone(XAPlusRollbackDoneEvent event) throws InterruptedException {
//            rollbackDoneEvents.put(event);
//        }
//
//        void postConstruct() {
//            threadPool.execute(this);
//            dispatcher.subscribe(this, XAPlusLogRollbackTransactionDecisionEvent.class);
//            dispatcher.subscribe(this, XAPlusRollbackBranchRequestEvent.class);
//            dispatcher.subscribe(this, XAPlusRollbackFailedEvent.class);
//            dispatcher.subscribe(this, XAPlusRollbackDoneEvent.class);
//        }
//    }
}
