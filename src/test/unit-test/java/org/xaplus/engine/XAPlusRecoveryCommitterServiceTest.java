package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.journal.XAPlusFindRecoveredXidStatusFailedEvent;
import org.xaplus.engine.events.journal.XAPlusFindRecoveredXidStatusRequestEvent;
import org.xaplus.engine.events.journal.XAPlusRecoveredXidStatusFoundEvent;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToCommitEvent;
import org.xaplus.engine.events.xaplus.XAPlusRemoteSuperiorOrderToRollbackEvent;
import org.xaplus.engine.events.xaplus.XAPlusRetryFromSuperiorRequestEvent;
import org.xaplus.engine.exceptions.XAPlusSystemException;
import org.xaplus.engine.stubs.XAConnectionStub;

import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusRecoveryCommitterServiceTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusRecoveryCommitterServiceTest.class);

    private XAPlusRecoveryCommitterService xaPlusRecoveryCommitterService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        xaPlusRecoveryCommitterService =
                new XAPlusRecoveryCommitterService(properties, threadPool, dispatcher, resources);
        xaPlusRecoveryCommitterService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusRecoveryCommitterService.finish();
        consumerStub.finish();
    }

    @Test
    public void testRecoveryCommitter() throws InterruptedException, SQLException, XAException, XAPlusSystemException {
        XAPlusXid xid1 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_1), XAPlusUid.generate(XA_PLUS_RESOURCE_1));
        XAPlusXid xid2 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_1), XAPlusUid.generate(XA_PLUS_RESOURCE_1));
        XAPlusXid xid3 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_1), XAPlusUid.generate(XA_PLUS_RESOURCE_1));
        XAPlusXid xid4 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_2), XAPlusUid.generate(XA_PLUS_RESOURCE_1));
        XAPlusXid xid5 = new XAPlusXid(XAPlusUid.generate(XA_PLUS_RESOURCE_3), XAPlusUid.generate(XA_PLUS_RESOURCE_1));
        logger.debug("xid1={}", xid1);
        logger.debug("xid2={}", xid2);
        logger.debug("xid3={}", xid3);
        logger.debug("xid4={}", xid4);
        logger.debug("xid5={}", xid5);
        // Start recovery
        Thread.sleep(1);
        long inFlightCutoff = System.currentTimeMillis();
        Set<XAPlusRecoveredResource> recoveredResources = new HashSet<>();
        XAPlusRecoveredResource recoveredResource1 = new XAPlusRecoveredResource(XA_RESOURCE_1,
                properties.getServerId(), inFlightCutoff, new XAConnectionStub(new Xid[]{xid1, xid2, xid3, xid4, xid5}));
        recoveredResource1.recovery();
        recoveredResources.add(recoveredResource1);
        dispatcher.dispatch(new XAPlusRecoveryPreparedEvent(recoveredResources));

        // Find status request, next commit, but commit failed
        XAPlusFindRecoveredXidStatusRequestEvent event11 = consumerStub
                .findRecoveredXidStatusRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event11);
        assertEquals(recoveredResource1.getUniqueName(), event11.getRecoveredResource().getUniqueName());
        dispatcher.dispatch(
                new XAPlusRecoveredXidStatusFoundEvent(event11.getXid(), event11.getRecoveredResource(), true));
        XAPlusCommitRecoveredXidRequestEvent event12 = consumerStub
                .commitRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event12);
        assertEquals(event12.getXid(), event11.getXid());
        dispatcher.dispatch(new XAPlusCommitRecoveredXidFailedEvent(event12.getXid()));

        // Find status request, next rollback, but rollback failed
        XAPlusFindRecoveredXidStatusRequestEvent event21 = consumerStub
                .findRecoveredXidStatusRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event21);
        assertEquals(recoveredResource1.getUniqueName(), event21.getRecoveredResource().getUniqueName());
        dispatcher.dispatch(
                new XAPlusRecoveredXidStatusFoundEvent(event21.getXid(), event21.getRecoveredResource(), false));
        XAPlusRollbackRecoveredXidRequestEvent event22 = consumerStub
                .rollbackRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event22);
        assertEquals(event22.getXid(), event21.getXid());
        dispatcher.dispatch(new XAPlusRollbackRecoveredXidFailedEvent(event22.getXid()));

        // Find status request failed
        XAPlusFindRecoveredXidStatusRequestEvent event3 = consumerStub
                .findRecoveredXidStatusRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event3);
        assertEquals(recoveredResource1.getUniqueName(), event3.getRecoveredResource().getUniqueName());
        dispatcher.dispatch(new XAPlusFindRecoveredXidStatusFailedEvent(event3.getXid(), event3.getRecoveredResource(),
                new Exception("find_exception")));

        // Retry request form superior, next order to commit, commit successful
        XAPlusRetryFromSuperiorRequestEvent event41 = consumerStub
                .retryFromSuperiorRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event41);
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToCommitEvent(event41.getXid()));
        XAPlusCommitRecoveredXidRequestEvent event42 = consumerStub
                .commitRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event42);
        assertEquals(event42.getXid(), event41.getXid());
        dispatcher.dispatch(new XAPlusRecoveredXidCommittedEvent(event42.getXid()));

        // Retry request from superior next order to rollback, rollback successful
        XAPlusRetryFromSuperiorRequestEvent event51 = consumerStub
                .retryFromSuperiorRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event51);
        dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(event51.getXid()));
        XAPlusRollbackRecoveredXidRequestEvent event52 = consumerStub
                .rollbackRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event52);
        assertEquals(event52.getXid(), event52.getXid());
        dispatcher.dispatch(new XAPlusRecoveredXidRolledBackEvent(event52.getXid()));

        // Wait final event
        XAPlusRecoveryFinishedEvent event = consumerStub
                .recoveryFinishedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
    }

    private class ConsumerStub extends Bolt implements
            XAPlusFindRecoveredXidStatusRequestEvent.Handler,
            XAPlusRetryFromSuperiorRequestEvent.Handler,
            XAPlusCommitRecoveredXidRequestEvent.Handler,
            XAPlusRollbackRecoveredXidRequestEvent.Handler,
            XAPlusRecoveryFinishedEvent.Handler {

        BlockingQueue<XAPlusFindRecoveredXidStatusRequestEvent> findRecoveredXidStatusRequestEvents;
        BlockingQueue<XAPlusRetryFromSuperiorRequestEvent> retryFromSuperiorRequestEvents;
        BlockingQueue<XAPlusCommitRecoveredXidRequestEvent> commitRecoveredXidRequestEvents;
        BlockingQueue<XAPlusRollbackRecoveredXidRequestEvent> rollbackRecoveredXidRequestEvents;
        BlockingQueue<XAPlusRecoveryFinishedEvent> recoveryFinishedEvents;

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);

            findRecoveredXidStatusRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            retryFromSuperiorRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            commitRecoveredXidRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackRecoveredXidRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            recoveryFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleFindRecoveredXidStatusRequest(XAPlusFindRecoveredXidStatusRequestEvent event) throws InterruptedException {
            findRecoveredXidStatusRequestEvents.put(event);
        }

        @Override
        public void handleRetryFromSuperiorRequest(XAPlusRetryFromSuperiorRequestEvent event) throws InterruptedException {
            retryFromSuperiorRequestEvents.put(event);
        }

        @Override
        public void handleCommitRecoveredXidRequest(XAPlusCommitRecoveredXidRequestEvent event) throws InterruptedException {
            commitRecoveredXidRequestEvents.put(event);
        }

        @Override
        public void handleRollbackRecoveredXidRequest(XAPlusRollbackRecoveredXidRequestEvent event) throws InterruptedException {
            rollbackRecoveredXidRequestEvents.put(event);
        }

        @Override
        public void handleRecoveryFinished(XAPlusRecoveryFinishedEvent event) throws InterruptedException {
            recoveryFinishedEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusFindRecoveredXidStatusRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRetryFromSuperiorRequestEvent.class);
            dispatcher.subscribe(this, XAPlusCommitRecoveredXidRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRecoveredXidRequestEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveryFinishedEvent.class);
        }
    }
}
