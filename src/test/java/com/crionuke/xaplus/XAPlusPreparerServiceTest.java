package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.XAPlusPrepareBranchRequestEvent;
import com.crionuke.xaplus.events.XAPlusPrepareTransactionEvent;
import com.crionuke.xaplus.events.twopc.XAPlus2pcRequestEvent;
import com.crionuke.xaplus.stubs.XAResourceStub;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusPreparerServiceTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPreparerServiceTest.class);

    XAPlusPreparerService xaPlusPreparerService;

    BlockingQueue<XAPlusPrepareBranchRequestEvent> prepareBranchRequestEvents;
    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(1);

        xaPlusPreparerService = new XAPlusPreparerService(properties, threadPool, dispatcher);
        xaPlusPreparerService.postConstruct();

        prepareBranchRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

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
        engine.begin();
        engine.enlistJdbc(XA_RESOURCE_1);
        engine.enlistJdbc(XA_RESOURCE_2);
        engine.enlistXAPlus(XA_PLUS_RESOURCE_1);
        XAPlusTransaction transaction = threadOfControl.getThreadContext().getTransaction();
        logger.info("Created transaction {}", transaction);
        dispatcher.dispatch(new XAPlus2pcRequestEvent(transaction));
        Set<XAPlusXid> branches = new HashSet<>();
        branches.addAll(transaction.getXaResources().keySet());
        branches.addAll(transaction.getXaPlusResources().keySet());
        // Poll events
        XAPlusPrepareBranchRequestEvent event1 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event1.getBranchXid());
        XAPlusPrepareBranchRequestEvent event2 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event2.getBranchXid());
        XAPlusPrepareBranchRequestEvent event3 =
                prepareBranchRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        branches.remove(event3.getBranchXid());
        // Check
        assertTrue(branches.isEmpty());
    }

    private class ConsumerStub extends Bolt implements XAPlusPrepareBranchRequestEvent.Handler {

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handlePrepareBranchRequest(XAPlusPrepareBranchRequestEvent event) throws InterruptedException {
            prepareBranchRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusPrepareBranchRequestEvent.class);
        }
    }
}
