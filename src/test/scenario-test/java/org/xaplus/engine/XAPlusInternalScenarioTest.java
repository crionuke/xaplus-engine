package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusScenarioSubordinateFailedEvent;
import org.xaplus.engine.events.XAPlusScenarioSubordinateFinishedEvent;
import org.xaplus.engine.events.XAPlusScenarioSuperiorFailedEvent;

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
        long value = initialRequest(false, false, false);
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check superior
        XAPlusScenarioSuperiorFailedEvent event1 = scenarioSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Check subordinate
        XAPlusScenarioSubordinateFailedEvent event2 = scenarioSubordinateFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
    }

    @Test
    public void testFromSuperiorToSubordinateCommitFailed() throws InterruptedException {
        // Setup scenario
        subordinateScenarioExceptions.commitException = true;
        long value = initialRequest(false, false, false);
        // Check superior
        XAPlusScenarioSuperiorFailedEvent event1 = scenarioSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check subordinate
        XAPlusScenarioSubordinateFailedEvent event2 = scenarioSubordinateFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
    }

    @Test
    public void testFromSubordinateToSuperiorDoneFailed() throws InterruptedException {
        // Setup scenario
        requestSuperiorExceptions.doneException = true;
        long value = initialRequest(false, false, false);
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check superior
        XAPlusScenarioSuperiorFailedEvent event1 = scenarioSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check subordinate
        XAPlusScenarioSubordinateFinishedEvent event2 = scenarioSubordinateFinishedEvents
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
