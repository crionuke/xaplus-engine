package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusRollbackDoneEvent;
import org.xaplus.engine.events.XAPlusRollbackFailedEvent;
import org.xaplus.engine.events.XAPlusRollbackRequestEvent;
import org.xaplus.engine.events.XAPlusTimeoutEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcDoneEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcFailedEvent;
import org.xaplus.engine.events.twopc.XAPlus2pcRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserCommitRequestEvent;
import org.xaplus.engine.events.user.XAPlusUserRollbackRequestEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusManagerServiceTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusManagerServiceTest.class);

    XAPlusManagerService xaPlusManagerService;

    BlockingQueue<XAPlus2pcRequestEvent> twoPcRequestEvents;
    BlockingQueue<XAPlusRollbackRequestEvent> rollbackRequestEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(1);

        xaPlusManagerService = new XAPlusManagerService(properties, threadPool, dispatcher);
        xaPlusManagerService.postConstruct();

        twoPcRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusManagerService.finish();
        consumerStub.finish();
    }

    @Test
    public void testUserCommitRequest() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        XAPlus2pcRequestEvent event = twoPcRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void testUserRollbackRequest() throws InterruptedException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserRollbackRequestEvent(transaction));
        XAPlusRollbackRequestEvent event = rollbackRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction, event.getTransaction());
    }

    @Test
    public void test2pcDone()
            throws InterruptedException, XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcDoneEvent(transaction));
        assertTrue(transaction.getFuture().get());
    }

    @Test
    public void testRollbackDone()
            throws InterruptedException, XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRollbackDoneEvent(transaction));
        assertTrue(transaction.getFuture().get());
    }

    @Test(expected = XAPlusCommitException.class)
    public void test2pcFailed()
            throws InterruptedException, XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlus2pcFailedEvent(transaction, new Exception("commit_exception")));
        transaction.getFuture().get();
    }

    @Test(expected = XAPlusRollbackException.class)
    public void testRollbackFailed()
            throws InterruptedException, XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusRollbackFailedEvent(transaction, new Exception("rollback_exception")));
        transaction.getFuture().get();
    }

    @Test(expected = XAPlusTimeoutException.class)
    public void testTimeout()
            throws InterruptedException, XAPlusCommitException, XAPlusRollbackException, XAPlusTimeoutException {
        XAPlusTransaction transaction = createTestSuperiorTransaction();
        dispatcher.dispatch(new XAPlusUserCommitRequestEvent(transaction));
        dispatcher.dispatch(new XAPlusTimeoutEvent(transaction));
        transaction.getFuture().get();
    }

    private class ConsumerStub extends Bolt implements
            XAPlus2pcRequestEvent.Handler,
            XAPlusRollbackRequestEvent.Handler{


        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handle2pcRequest(XAPlus2pcRequestEvent event) throws InterruptedException {
            twoPcRequestEvents.put(event);
        }

        @Override
        public void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException {
            rollbackRequestEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlus2pcRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRequestEvent.class);
        }
    }
}
