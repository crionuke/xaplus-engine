package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusTestSubordinateFailedEvent;
import org.xaplus.engine.events.XAPlusTestSubordinateFinishedEvent;
import org.xaplus.engine.events.XAPlusTestSuperiorFailedEvent;

import java.util.concurrent.TimeUnit;

public class XAPlusInternalScenarioTest extends XAPlusScenarioTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusInternalScenarioTest.class);

    @Before
    public void beforeTest() {
        createComponents();
        start();
    }

    @Test
    public void testFromSubrodinateToSuperiorReadyFailed() throws InterruptedException {
        // Setup scenario
        requestSuperiorExceptions.readiedException = true;
        long value = startGlobalScenario(false, false, false);
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
    public void testFromSuperiorToSubordinateCommitFailed() throws InterruptedException {
        // Setup scenario
        subordinateScenarioExceptions.commitException = true;
        long value = startGlobalScenario(false, false, false);
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
    public void testFromSubordinateToSuperiorDoneFailed() throws InterruptedException {
        // Setup scenario
        requestSuperiorExceptions.doneException = true;
        long value = startGlobalScenario(false, false, false);
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check superior
        XAPlusTestSuperiorFailedEvent event1 = testSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check subordinate
        XAPlusTestSubordinateFinishedEvent event2 = testSubordinateFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertTrue(event2.getStatus());
        assertEquals(value, event2.getValue());
        // Reset scenario exceptions
        requestSuperiorExceptions.reset();
        // Superior recovery
        superiorXAPlus.engine.startRecovery();
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000);
    }
}
