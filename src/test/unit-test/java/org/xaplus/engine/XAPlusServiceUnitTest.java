package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.recovery.*;
import org.xaplus.engine.events.xaplus.XAPlusReadyStatusReportedEvent;
import org.xaplus.engine.events.xaplus.XAPlusReportReadyStatusFailedEvent;
import org.xaplus.engine.events.xaplus.XAPlusReportReadyStatusRequestEvent;
import org.xaplus.engine.stubs.XAPlusResourceStub;

import javax.transaction.xa.XAException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusServiceUnitTest extends XAPlusUnitTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusServiceUnitTest.class);

    private XAPlusService xaPlusService;
    private ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        xaPlusService = new XAPlusService(properties, threadPool, dispatcher);
        xaPlusService.postConstruct();
        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusService.finish();
        consumerStub.finish();
    }

    @Test
    public void testCommitRecoveredXidRequestSuccessfully() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        Mockito.verify(xaPlusResourceMock, Mockito.timeout(VERIFY_MS)).commit(branchXid, false);
//        XAPlusRecoveredXidCommittedEvent event =
//                consumerStub.recoveredXidCommittedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event);
//        assertEquals(branchXid, event.getXid());
//        assertEquals(XA_RESOURCE_1, event.getUniqueName());
    }

    @Test
    public void testCommitRecoveredXidRequestXAERNOTA() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAException(XAException.XAER_NOTA))
//                .when(xaPlusResourceMock).commit(branchXid, false);
//        dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        XAPlusRecoveredXidCommittedEvent event =
//                consumerStub.recoveredXidCommittedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event);
//        assertEquals(branchXid, event.getXid());
//        assertEquals(XA_RESOURCE_1, event.getUniqueName());
    }

    @Test
    public void testCommitRecoveredXidRequestXAHEURCOM() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAException(XAException.XA_HEURCOM))
//                .when(xaPlusResourceMock).commit(branchXid, false);
//        dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        XAPlusForgetRecoveredXidRequestEvent event1 =
//                consumerStub.forgetRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event1);
//        assertEquals(branchXid, event1.getXid());
//        XAPlusRecoveredXidCommittedEvent event2 =
//                consumerStub.recoveredXidCommittedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event2);
//        assertEquals(branchXid, event2.getXid());
//        assertEquals(XA_RESOURCE_1, event2.getUniqueName());
    }

    @Test
    public void testCommitRecoveredXidRequestXAHEURHAZ() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAException(XAException.XA_HEURHAZ))
//                .when(xaPlusResourceMock).commit(branchXid, false);
//        dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        XAPlusForgetRecoveredXidRequestEvent event1 =
//                consumerStub.forgetRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event1);
//        assertEquals(branchXid, event1.getXid());
//        XAPlusCommitRecoveredXidFailedEvent event2 =
//                consumerStub.commitRecoveredXidFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event2);
//        assertEquals(branchXid, event2.getXid());
//        assertEquals(XA_RESOURCE_1, event2.getUniqueName());
    }

    @Test
    public void testCommitRecoveredXidRequestUnknown() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAException(XAException.XA_RBROLLBACK))
//                .when(xaPlusResourceMock).commit(branchXid, false);
//        dispatcher.dispatch(new XAPlusCommitRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        XAPlusCommitRecoveredXidFailedEvent event =
//                consumerStub.commitRecoveredXidFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event);
//        assertEquals(branchXid, event.getXid());
//        assertEquals(XA_RESOURCE_1, event.getUniqueName());
    }

    @Test
    public void testRollbackRecoveredXidRequestSuccessfully() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        Mockito.verify(xaPlusResourceMock, Mockito.timeout(VERIFY_MS)).rollback(branchXid);
//        XAPlusRecoveredXidRolledBackEvent event =
//                consumerStub.recoveredXidRolledBackEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event);
//        assertEquals(branchXid, event.getXid());
//        assertEquals(XA_RESOURCE_1, event.getUniqueName());
    }

    @Test
    public void testRollbackRecoveredXidRequestXAERNOTA() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAException(XAException.XAER_NOTA))
//                .when(xaPlusResourceMock).rollback(branchXid);
//        dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        XAPlusRecoveredXidRolledBackEvent event =
//                consumerStub.recoveredXidRolledBackEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event);
//        assertEquals(branchXid, event.getXid());
//        assertEquals(XA_RESOURCE_1, event.getUniqueName());
    }

    @Test
    public void testRollbackRecoveredXidRequestXAHEURRB() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAException(XAException.XA_HEURRB))
//                .when(xaPlusResourceMock).rollback(branchXid);
//        dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        XAPlusForgetRecoveredXidRequestEvent event1 =
//                consumerStub.forgetRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event1);
//        assertEquals(branchXid, event1.getXid());
//        XAPlusRecoveredXidRolledBackEvent event2 =
//                consumerStub.recoveredXidRolledBackEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event2);
//        assertEquals(branchXid, event2.getXid());
//        assertEquals(XA_RESOURCE_1, event2.getUniqueName());
    }

    @Test
    public void testRollbackRecoveredXidRequestXAHEURHAZ() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAException(XAException.XA_HEURHAZ))
//                .when(xaPlusResourceMock).rollback(branchXid);
//        dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        XAPlusForgetRecoveredXidRequestEvent event1 =
//                consumerStub.forgetRecoveredXidRequestEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event1);
//        assertEquals(branchXid, event1.getXid());
//        XAPlusRollbackRecoveredXidFailedEvent event2 =
//                consumerStub.rollbackRecoveredXidFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event2);
//        assertEquals(branchXid, event2.getXid());
//        assertEquals(XA_RESOURCE_1, event2.getUniqueName());
    }

    @Test
    public void testRollbackRecoveredXidRequestUnknown() throws InterruptedException, XAException {
//        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
//        XAPlusXid branchXid = createJdbcXid(transaction);
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAException(XAException.XA_RBROLLBACK))
//                .when(xaPlusResourceMock).rollback(branchXid);
//        dispatcher.dispatch(new XAPlusRollbackRecoveredXidRequestEvent(branchXid, xaPlusResourceMock, XA_RESOURCE_1));
//        XAPlusRollbackRecoveredXidFailedEvent event =
//                consumerStub.rollbackRecoveredXidFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
//        assertNotNull(event);
//        assertEquals(branchXid, event.getXid());
//        assertEquals(XA_RESOURCE_1, event.getUniqueName());
    }

    @Test
    public void testReportReadyStatusRequestEventSuccessfully() throws InterruptedException, XAPlusException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_2);
        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
        dispatcher.dispatch(new XAPlusReportReadyStatusRequestEvent(transaction.getXid(), xaPlusResourceMock));
        Mockito.verify(xaPlusResourceMock, Mockito.timeout(VERIFY_MS)).ready(transaction.getXid());
        XAPlusReadyStatusReportedEvent event = consumerStub.readyStatusReportedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getXid());
    }

    @Test
    public void testReportReadyStatusRequestEventFailed() throws InterruptedException, XAPlusException {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_2);
        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
        Mockito.doThrow(new XAPlusException("ready_exception")).when(xaPlusResourceMock).ready(transaction.getXid());
        dispatcher.dispatch(new XAPlusReportReadyStatusRequestEvent(transaction.getXid(), xaPlusResourceMock));
        XAPlusReportReadyStatusFailedEvent event =
                consumerStub.reportReadyStatusFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(transaction.getXid(), event.getXid());
    }

    @Test
    public void testRetryFromSuperiorRequestSuccessfully() throws InterruptedException, XAPlusException {
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        dispatcher.dispatch(new XAPlusRetryFromSuperiorRequestEvent(XA_PLUS_RESOURCE_1, xaPlusResourceMock));
//        Mockito.verify(xaPlusResourceMock, Mockito.timeout(VERIFY_MS)).retry(properties.getServerId());
    }

    @Test
    public void testRetryFromSuperiorRequestFailed() throws InterruptedException, XAPlusException {
//        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
//        Mockito.doThrow(new XAPlusException("retry_exception"))
//                .when(xaPlusResourceMock).retry(properties.getServerId());
//        dispatcher.dispatch(new XAPlusRetryFromSuperiorRequestEvent(XA_PLUS_RESOURCE_1, xaPlusResourceMock));
//        Thread.sleep(1000);
    }

    private class ConsumerStub extends Bolt implements
            XAPlusRecoveredXidCommittedEvent.Handler,
            XAPlusCommitRecoveredXidFailedEvent.Handler,
            XAPlusRecoveredXidRolledBackEvent.Handler,
            XAPlusRollbackRecoveredXidFailedEvent.Handler,
            XAPlusForgetRecoveredXidRequestEvent.Handler,
            XAPlusReadyStatusReportedEvent.Handler,
            XAPlusReportReadyStatusFailedEvent.Handler {

        BlockingQueue<XAPlusRecoveredXidCommittedEvent> recoveredXidCommittedEvents;
        BlockingQueue<XAPlusCommitRecoveredXidFailedEvent> commitRecoveredXidFailedEvents;
        BlockingQueue<XAPlusRecoveredXidRolledBackEvent> recoveredXidRolledBackEvents;
        BlockingQueue<XAPlusRollbackRecoveredXidFailedEvent> rollbackRecoveredXidFailedEvents;
        BlockingQueue<XAPlusForgetRecoveredXidRequestEvent> forgetRecoveredXidRequestEvents;
        BlockingQueue<XAPlusReadyStatusReportedEvent> readyStatusReportedEvents;
        BlockingQueue<XAPlusReportReadyStatusFailedEvent> reportReadyStatusFailedEvents;

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
            recoveredXidCommittedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            commitRecoveredXidFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            recoveredXidRolledBackEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            rollbackRecoveredXidFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            forgetRecoveredXidRequestEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            readyStatusReportedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            reportReadyStatusFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleRecoveredXidCommitted(XAPlusRecoveredXidCommittedEvent event) throws InterruptedException {
            recoveredXidCommittedEvents.put(event);
        }

        @Override
        public void handleCommitRecoveredXidFailed(XAPlusCommitRecoveredXidFailedEvent event)
                throws InterruptedException {
            commitRecoveredXidFailedEvents.put(event);
        }

        @Override
        public void handleRecoveredXidRolledBack(XAPlusRecoveredXidRolledBackEvent event) throws InterruptedException {
            recoveredXidRolledBackEvents.put(event);
        }

        @Override
        public void handleRollbackRecoveredXidFailed(XAPlusRollbackRecoveredXidFailedEvent event) throws InterruptedException {
            rollbackRecoveredXidFailedEvents.put(event);
        }

        @Override
        public void handleForgetRecoveredXidRequest(XAPlusForgetRecoveredXidRequestEvent event)
                throws InterruptedException {
            forgetRecoveredXidRequestEvents.put(event);
        }

        @Override
        public void handleReadyStatusReported(XAPlusReadyStatusReportedEvent event) throws InterruptedException {
            readyStatusReportedEvents.put(event);
        }

        @Override
        public void handleReportReadyStatusFailed(XAPlusReportReadyStatusFailedEvent event)
                throws InterruptedException {
            reportReadyStatusFailedEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusRecoveredXidCommittedEvent.class);
            dispatcher.subscribe(this, XAPlusCommitRecoveredXidFailedEvent.class);
            dispatcher.subscribe(this, XAPlusRecoveredXidRolledBackEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRecoveredXidFailedEvent.class);
            dispatcher.subscribe(this, XAPlusForgetRecoveredXidRequestEvent.class);
            dispatcher.subscribe(this, XAPlusReadyStatusReportedEvent.class);
            dispatcher.subscribe(this, XAPlusReportReadyStatusFailedEvent.class);
        }
    }
}
