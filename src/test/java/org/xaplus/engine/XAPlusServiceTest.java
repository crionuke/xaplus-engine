package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;
import org.xaplus.engine.stubs.XAPlusResourceStub;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusServiceTest extends XAPlusTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusServiceTest.class);

    XAPlusService xaPlusService;

    BlockingQueue<XAPlusReadyStatusReportedEvent> readyStatusReportedEvents;
    BlockingQueue<XAPlusReportReadyStatusFailedEvent> reportReadyStatusFailedEvents;
    BlockingQueue<XAPlusDoneStatusReportedEvent> doneStatusReportedEvents;
    BlockingQueue<XAPlusReportDoneStatusFailedEvent> reportDoneStatusFailedEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() {
        createXAPlusComponents(1);

        xaPlusService = new XAPlusService(properties, threadPool, dispatcher);
        xaPlusService.postConstruct();

        readyStatusReportedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        reportReadyStatusFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        doneStatusReportedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        reportDoneStatusFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusService.finish();
        consumerStub.finish();
    }

    @Test
    public void testReportReadyStatusRequestEventSuccessfully() throws InterruptedException, XAPlusException {
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid branchXid = createBranchXid(transaction);
        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
        dispatcher.dispatch(new XAPlusReportReadyStatusRequestEvent(branchXid, xaPlusResourceMock));
        Mockito.verify(xaPlusResourceMock, Mockito.timeout(VERIFY_MS)).ready(branchXid);
        XAPlusReadyStatusReportedEvent event = readyStatusReportedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(branchXid, event.getXid());
    }

    @Test
    public void testReportReadyStatusRequestEventFailed() throws InterruptedException, XAPlusException {
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid branchXid = createBranchXid(transaction);
        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
        Mockito.doThrow(new XAPlusException("ready_exception")).when(xaPlusResourceMock).ready(branchXid);
        dispatcher.dispatch(new XAPlusReportReadyStatusRequestEvent(branchXid, xaPlusResourceMock));
        XAPlusReportReadyStatusFailedEvent event =
                reportReadyStatusFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(branchXid, event.getXid());
    }

    @Test
    public void testReportDoneStatusRequestSuccessfully() throws InterruptedException, XAPlusException {
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid branchXid = createBranchXid(transaction);
        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
        dispatcher.dispatch(new XAPlusReportDoneStatusRequestEvent(branchXid, xaPlusResourceMock));
        Mockito.verify(xaPlusResourceMock, Mockito.timeout(VERIFY_MS)).done(branchXid);
        XAPlusDoneStatusReportedEvent event = doneStatusReportedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(branchXid, event.getXid());
    }

    @Test
    public void testReportDoneStatusRequestFailed() throws InterruptedException, XAPlusException {
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid branchXid = createBranchXid(transaction);
        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
        Mockito.doThrow(new XAPlusException("done_exception")).when(xaPlusResourceMock).done(branchXid);
        dispatcher.dispatch(new XAPlusReportDoneStatusRequestEvent(branchXid, xaPlusResourceMock));
        XAPlusReportDoneStatusFailedEvent event =
                reportDoneStatusFailedEvents.poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event);
        assertEquals(branchXid, event.getXid());
    }

    @Test
    public void testRetryFromSuperiorRequestSuccessfully() throws InterruptedException, XAPlusException {
        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
        dispatcher.dispatch(new XAPlusRetryFromSuperiorRequestEvent(xaPlusResourceMock));
        Mockito.verify(xaPlusResourceMock, Mockito.timeout(VERIFY_MS)).retry(properties.getServerId());
    }

    @Test
    public void testRetryFromSuperiorRequestFailed() throws InterruptedException, XAPlusException {
        XAPlusResource xaPlusResourceMock = Mockito.mock(XAPlusResourceStub.class);
        Mockito.doThrow(new XAPlusException("retry_exception"))
                .when(xaPlusResourceMock).retry(properties.getServerId());
        dispatcher.dispatch(new XAPlusRetryFromSuperiorRequestEvent(xaPlusResourceMock));
        Thread.sleep(1000);
    }

    private class ConsumerStub extends Bolt implements
            XAPlusReadyStatusReportedEvent.Handler,
            XAPlusReportReadyStatusFailedEvent.Handler,
            XAPlusDoneStatusReportedEvent.Handler,
            XAPlusReportDoneStatusFailedEvent.Handler {

        ConsumerStub() {
            super("stub-consumer", QUEUE_SIZE);
        }

        @Override
        public void handleReadyStatusReported(XAPlusReadyStatusReportedEvent event) throws InterruptedException {
            readyStatusReportedEvents.put(event);
        }

        @Override
        public void handleReportReadyStatusFailed(XAPlusReportReadyStatusFailedEvent event) throws InterruptedException {
            reportReadyStatusFailedEvents.put(event);
        }

        @Override
        public void handleDoneStatusReported(XAPlusDoneStatusReportedEvent event) throws InterruptedException {
            doneStatusReportedEvents.put(event);
        }

        @Override
        public void handleReportDoneStatusFailed(XAPlusReportDoneStatusFailedEvent event) throws InterruptedException {
            reportDoneStatusFailedEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusReadyStatusReportedEvent.class);
            dispatcher.subscribe(this, XAPlusReportReadyStatusFailedEvent.class);
            dispatcher.subscribe(this, XAPlusDoneStatusReportedEvent.class);
            dispatcher.subscribe(this, XAPlusReportDoneStatusFailedEvent.class);
        }
    }
}
