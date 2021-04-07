package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;

import java.util.concurrent.TimeUnit;

public class XAPlusUserScenarioTest extends XAPlusScenarioTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusUserScenarioTest.class);

    @Before
    public void beforeTest() {
        createComponents();
        construct();
    }

    @Test
    public void testCommitLocalTransaction() throws InterruptedException {
        long value = startLocalTransaction();
        // Check transaction
        XAPlusLocalTransactionFinishedEvent event1 = localTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
    }

    @Test
    public void testCommitDistributedTransaction() throws InterruptedException {
        long value = startDistributedTransaction();
        // Check transaction
        XAPlusDistributedTransactionFinishedEvent event1 = distributedTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
    }

    @Test
    public void testSuperiorCommitTransaction() throws InterruptedException {
        long value = startGlobalTransaction(false, false, false);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
        // Check subordinate
        XAPlusTestSubordinateFinishedEvent event2 = testSubordinateFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        assertTrue(event2.getStatus());
    }

    @Test
    public void testSuperiorCommitTransactionAndReportReadyStatusFromSubordinateToSuperiorFailed()
            throws InterruptedException {
        // Setup scenario
        requestSuperiorExceptions.readiedException = true;
        long value = startGlobalTransaction(false, false, false);
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check superior
        XAPlusTestSuperiorFailedEvent event1 = testSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Check subordinate
        XAPlusTestSubordinateFailedEvent event2 = testSubordinateFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
    }

    @Test
    public void testSuperiorCommitRequestToSubordinateFailed() throws InterruptedException {
        // Setup scenario
        requestSubordinateExceptions.commitException = true;
        long value = startGlobalTransaction(false, false, false);
        // Check superior
        XAPlusTestSuperiorFailedEvent event1 = testSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check subordinate
        XAPlusTestSubordinateFailedEvent event2 = testSubordinateFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
    }

    @Test
    public void testSuperiorRollbackBeforeRequest() throws InterruptedException {
        long value = startGlobalTransaction(true, false, false);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    @Test
    public void testSuperiorRollbackBeforeCommit() throws InterruptedException {
        long value = startGlobalTransaction(false, true, false);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    @Test
    public void testSubordinateRollbackBeforeCommit() throws InterruptedException {
        long value = startGlobalTransaction(false, false, true);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
        // Check subordinate
        XAPlusTestSubordinateFinishedEvent event2 = testSubordinateFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        assertFalse(event2.getStatus());
    }

    @Test
    public void testSubordinateRollbackBeforeCommitAndReportFailedStatusToSuperiorFailed() throws InterruptedException {
        // Setup scenario
        requestSuperiorExceptions.failedException = true;
        long value = startGlobalTransaction(false, false, true);
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check superior
        XAPlusTestSuperiorFailedEvent event1 = testSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Check subordinate
        XAPlusTestSubordinateFailedEvent event2 = testSubordinateFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        // Superior recovery
        superiorXAPlus.engine.startRecovery();
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000);
    }
}
