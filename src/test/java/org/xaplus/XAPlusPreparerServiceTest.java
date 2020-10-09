package org.xaplus;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.events.*;
import org.xaplus.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.events.xaplus.XAPlusRemoteSubordinateReadyEvent;
import org.xaplus.stubs.XAPlusResourceStub;
import org.xaplus.stubs.XAResourceStub;

import javax.transaction.xa.XAException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusPreparerServiceTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPreparerServiceTest.class);

    XAPlusPreparerService xaPlusPreparerService;

    BlockingQueue<XAPlusPrepareBranchRequestEvent> prepareBranchRequestEvents;
    BlockingQueue<XAPlusTransactionPreparedEvent> transactionPreparedEvents;
    BlockingQueue<XAPlus2pcFailedEvent> twoPcFailedEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(1);

        xaPlusPreparerService = new XAPlusPreparerService(properties, threadPool, dispatcher);
        xaPlusPreparerService.postConstruct();

        prepareBranchRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        transactionPreparedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        twoPcFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusPreparerService.finish();
        consumerStub.finish();
    }

    @Test
    public void test2pcRequest() throws InterruptedException, SQLException, XAException {
        // Create transaction and branches for xa and xa+ resources
        Set<XAPlusXid> branches = new HashSet<>();
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid bxid1 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(),
                properties.getServerId());
        transaction.enlist(bxid1, "db1", new XAResourceStub());
        branches.add(bxid1);
        XAPlusXid bxid2 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(),
                properties.getServerId());
        transaction.enlist(bxid2, "db2", new XAResourceStub());
        branches.add(bxid2);
        XAPlusXid bxid3 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), "service1");
        transaction.enlist(bxid3, "service1", new XAPlusResourceStub());
        branches.add(bxid3);
        logger.info("Transaction {} created", transaction);
        logger.info("Branch1 has xid={}", bxid1);
        logger.info("Branch2 has xid={}", bxid2);
        logger.info("Branch3 has xid={}", bxid3);
        // Request transaction preparation
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        // Waiting preparation requests for all branches
        XAPlusPrepareBranchRequestEvent event1 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event1.getBranchXid());
        XAPlusPrepareBranchRequestEvent event2 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event2.getBranchXid());
        XAPlusPrepareBranchRequestEvent event3 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event3.getBranchXid());
        // Check events
        assertTrue(branches.isEmpty());
    }

    @Test
    public void testPrepareTransaction() throws InterruptedException, SQLException, XAException {
        // Create transaction and branches for xa and xa+ resources
        Set<XAPlusXid> branches = new HashSet<>();
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid bxid1 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(),
                properties.getServerId());
        transaction.enlist(bxid1, "db1", new XAResourceStub());
        branches.add(bxid1);
        XAPlusXid bxid2 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(),
                properties.getServerId());
        transaction.enlist(bxid2, "db2", new XAResourceStub());
        branches.add(bxid2);
        XAPlusXid bxid3 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), "service1");
        transaction.enlist(bxid3, "service1", new XAPlusResourceStub());
        branches.add(bxid3);
        logger.info("Transaction {} created", transaction);
        logger.info("Branch1 has xid={}", bxid1);
        logger.info("Branch2 has xid={}", bxid2);
        logger.info("Branch3 has xid={}", bxid3);
        // Request transaction preparation
        dispatcher.dispatch(new XAPlusPrepareTransactionEvent(transaction));
        // Waiting preparation requests for all branches
        XAPlusPrepareBranchRequestEvent event1 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event1.getBranchXid());
        XAPlusPrepareBranchRequestEvent event2 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event2.getBranchXid());
        XAPlusPrepareBranchRequestEvent event3 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event3.getBranchXid());
        // Check events
        assertTrue(branches.isEmpty());
    }

    @Test
    public void testTransactionPrepared() throws InterruptedException {
        // Create transaction and branches for xa and xa+ resources
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid bxid1 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(),
                properties.getServerId());
        transaction.enlist(bxid1, "db1", new XAResourceStub());
        XAPlusXid bxid2 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(),
                properties.getServerId());
        transaction.enlist(bxid2, "db2", new XAResourceStub());
        XAPlusXid bxid3 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(), "service1");
        transaction.enlist(bxid3, "service1", new XAPlusResourceStub());
        logger.info("Transaction {} created", transaction);
        logger.info("Branch1 has xid={}", bxid1);
        logger.info("Branch2 has xid={}", bxid2);
        logger.info("Branch3 has xid={}", bxid3);
        // Request to transaction preparation
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        // Emulate branch preparation
        dispatcher.dispatch(new XAPlusBranchPreparedEvent(transaction.getXid(), bxid1));
        dispatcher.dispatch(new XAPlusBranchReadOnlyEvent(transaction.getXid(), bxid2));
        dispatcher.dispatch(new XAPlusRemoteSubordinateReadyEvent(bxid3));
        // Wait transaction prepared event
        XAPlusTransactionPreparedEvent event = transactionPreparedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        // Check that it our transaction
        assertEquals(event.getTransaction().getXid(), transaction.getXid());
    }

    @Test
    public void testPrepareBranchFailed() throws InterruptedException {
        // Create transaction and branch for xa resource
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid bxid1 = uidGenerator.generateXid(transaction.getXid().getGlobalTransactionIdUid(),
                properties.getServerId());
        transaction.enlist(bxid1, "db1", new XAResourceStub());
        logger.info("Transaction {} created", transaction);
        logger.info("Branch has xid={}", bxid1);
        // Request to transaction preparation
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        // Emulate preparation fail
        dispatcher.dispatch(new XAPlusPrepareBranchFailedEvent(transaction.getXid(), bxid1, new Exception("failed")));
        // Wait 2pc failed event
        XAPlus2pcFailedEvent event = twoPcFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        // Check that it our transaction
        assertEquals(event.getTransaction().getXid(), transaction.getXid());
    }

    private class ConsumerStub extends Bolt implements
            XAPlusPrepareBranchRequestEvent.Handler,
            XAPlusTransactionPreparedEvent.Handler,
            XAPlus2pcFailedEvent.Handler {

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handlePrepareBranchRequest(XAPlusPrepareBranchRequestEvent event) throws InterruptedException {
            prepareBranchRequestEvents.put(event);
        }

        @Override
        public void handleTransactionPrepared(XAPlusTransactionPreparedEvent event) throws InterruptedException {
            transactionPreparedEvents.put(event);
        }

        @Override
        public void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException {
            twoPcFailedEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusPrepareBranchRequestEvent.class);
            dispatcher.subscribe(this, XAPlusTransactionPreparedEvent.class);
            dispatcher.subscribe(this, XAPlus2pcFailedEvent.class);
        }
    }
}
