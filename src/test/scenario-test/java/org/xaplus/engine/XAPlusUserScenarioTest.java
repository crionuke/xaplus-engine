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
    public void testUserCommitLocalScenario() throws InterruptedException {
        long value = startLocalScenario();
        // Check superior
        XAPlusLocalTransactionFinishedEvent event1 = localTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
    }

    @Test
    public void testUserCommitDistributedScenario() throws InterruptedException {
        long value = startDistributedScenario();
        // Check superior
        XAPlusDistributedTransactionFinishedEvent event1 = distributedTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
    }

    @Test
    public void testUserCommitScenario() throws InterruptedException {
        long value = startGlobalScenario(false, false, false);
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
    public void testSuperiorUserRollbackBeforeRequestScenario() throws InterruptedException {
        long value = startGlobalScenario(true, false, false);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    @Test
    public void testSuperiorUserRollbackBeforeCommitScenario() throws InterruptedException {
        long value = startGlobalScenario(false, true, false);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    @Test
    public void testSubordinateUserRollbackBeforeCommitScenario() throws InterruptedException {
        long value = startGlobalScenario(false, false, true);
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
    public void testSubordinateUserRollbackBeforeCommitAndReportFailedStatusFailed() throws InterruptedException {
        // Setup scenario
        requestSuperiorExceptions.failedException = true;
        long value = startGlobalScenario(false, false, true);
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
