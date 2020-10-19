package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusCommitTransactionEvent;
import org.xaplus.engine.events.XAPlusTransactionPreparedEvent;
import org.xaplus.engine.events.journal.XAPlusCommitTransactionDecisionLoggedEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionEvent;
import org.xaplus.engine.events.journal.XAPlusLogCommitTransactionDecisionFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.xa.XAPlusBranchCommittedEvent;
import org.xaplus.engine.events.xa.XAPlusCommitBranchFailedEvent;
import org.xaplus.engine.events.xa.XAPlusCommitBranchRequestEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSubordinateDoneEvent;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusCommitterServiceTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusCommitterServiceTest.class);

    XAPlusCommitterService xaPlusCommitterService;

    BlockingQueue<XAPlusLogCommitTransactionDecisionEvent> logCommitTransactionDecisionEvents;
    BlockingQueue<XAPlusCommitBranchRequestEvent> commitBranchRequestEvents;
    BlockingQueue<XAPlus2pcFailedEvent> twoPcFailedEvents;
    BlockingQueue<XAPlus2pcDoneEvent> twoPcDoneEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);

        xaPlusCommitterService = new XAPlusCommitterService(properties, threadPool, dispatcher);
        xaPlusCommitterService.postConstruct();

        logCommitTransactionDecisionEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitBranchRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        twoPcFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        twoPcDoneEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusCommitterService.finish();
        consumerStub.finish();
    }

    @Test
    public void testTransactionPrepared() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusTransactionPreparedEvent(transaction));
        XAPlusLogCommitTransactionDecisionEvent event =
                logCommitTransactionDecisionEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void testCommitTransaction() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusCommitTransactionEvent(transaction));
        XAPlusLogCommitTransactionDecisionEvent event =
                logCommitTransactionDecisionEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void testCommitTransactionDecisionLogged() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusCommitTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusCommitTransactionDecisionLoggedEvent(transaction));
        Set<XAPlusXid> branches = new HashSet<>();
        branches.addAll(transaction.getXaResources().keySet());
        branches.addAll(transaction.getXaPlusResources().keySet());
        while (!branches.isEmpty()) {
            XAPlusCommitBranchRequestEvent event =
                    commitBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
            assertNotNull(event);
            branches.remove(event.getBranchXid());
        }
        assertTrue(branches.isEmpty());
    }

    @Test
    public void testCommitTransactionDecisionFailed() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusCommitTransactionEvent(transaction));
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionFailedEvent(transaction,
                new Exception("log_exception")));
        XAPlus2pcFailedEvent event = twoPcFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void test2pcCommitDone() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusCommitTransactionEvent(transaction));
        for (XAPlusXid bxid : transaction.getXaResources().keySet()) {
            dispatcher.dispatch(new XAPlusBranchCommittedEvent(transaction.getXid(), bxid));
        }
        for (XAPlusXid bxid : transaction.getXaPlusResources().keySet()) {
            dispatcher.dispatch(new XAPlusRemoteSubordinateDoneEvent(bxid));
        }
        XAPlus2pcDoneEvent event = twoPcDoneEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void testCommitBranchFailed() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusCommitTransactionEvent(transaction));
        for (XAPlusXid bxid : transaction.getXaResources().keySet()) {
            dispatcher.dispatch(new XAPlusCommitBranchFailedEvent(transaction.getXid(), bxid,
                    new Exception("commit_exception")));
        }
        XAPlus2pcFailedEvent event = twoPcFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusLogCommitTransactionDecisionEvent.Handler,
            XAPlusCommitBranchRequestEvent.Handler,
            XAPlus2pcFailedEvent.Handler,
            XAPlus2pcDoneEvent.Handler {

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handleLogCommitTransactionDecision(XAPlusLogCommitTransactionDecisionEvent event)
                throws InterruptedException {
            logCommitTransactionDecisionEvents.put(event);
        }

        @Override
        public void handleCommitBranchRequest(XAPlusCommitBranchRequestEvent event) throws InterruptedException {
            commitBranchRequestEvents.put(event);
        }

        @Override
        public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
            twoPcFailedEvents.put(event);
        }

        @Override
        public void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException {
            twoPcDoneEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusLogCommitTransactionDecisionEvent.class);
            dispatcher.subscribe(this, XAPlusCommitBranchRequestEvent.class);
            dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
            dispatcher.subscribe(this, XAPlus2pcDoneEvent.class);
        }
    }
}
