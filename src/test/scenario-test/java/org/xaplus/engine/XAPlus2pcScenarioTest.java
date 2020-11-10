package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;

import java.util.concurrent.TimeUnit;

public class XAPlus2pcScenarioTest extends XAPlusScenarioTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlus2pcScenarioTest.class);

    @Before
    public void beforeTest() {
        createComponents();
        start();
    }

    @Test
    public void testUserCommitScenario() throws InterruptedException {
        long value = initialRequest(false, false, false);
        // Check superior
        XAPlusScenarioSuperiorFinishedEvent event1 = scenarioSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
        // Check subordinate
        XAPlusScenarioSubordinateFinishedEvent event2 = scenarioSubordinateFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        assertTrue(event2.getStatus());
    }

    @Test
    public void testUserRollbackBeforeRequestScenario() throws InterruptedException {
        long value = initialRequest(true, false, false);
        // Check superior
        XAPlusScenarioSuperiorFinishedEvent event1 = scenarioSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    @Test
    public void testUserRollbackBeforeCommitScenario() throws InterruptedException {
        long value = initialRequest(false, true, false);
        // Check superior
        XAPlusScenarioSuperiorFinishedEvent event1 = scenarioSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    @Test
    public void testSuperiorCommitButSubordinateRollbackScenario() throws InterruptedException {
        long value = initialRequest(false, false, true);
        // Check superior
        XAPlusScenarioSuperiorFinishedEvent event1 = scenarioSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
        // Check subordinate
        XAPlusScenarioSubordinateFinishedEvent event2 = scenarioSubordinateFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        assertFalse(event2.getStatus());
    }

    @Test
    public void testFromSubrodinateToSuperiorReadyFailed() throws InterruptedException {
        // Setup scenario
        superiorScenario.readiedException = true;
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
        subordinateScenario.commitException = true;
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

    long initialRequest(boolean superiorBeforeRequestException,
                        boolean superiorBeforeCommitException,
                        boolean subordinateBeforeCommitException) throws InterruptedException {
        long value = Math.round(100000 + Math.random() * 899999);
        testDispatcher.dispatch(
                new XAPlusScenarioInitialRequestEvent(value, superiorBeforeRequestException,
                        superiorBeforeCommitException,
                        subordinateBeforeCommitException));
        return value;
    }
}
