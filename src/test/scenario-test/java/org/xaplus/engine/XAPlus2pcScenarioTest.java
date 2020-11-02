package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusScenarioSubordinateFinishedEvent;
import org.xaplus.engine.events.XAPlusScenarioSuperiorFailedEvent;
import org.xaplus.engine.events.XAPlusScenarioSuperiorFinishedEvent;
import org.xaplus.engine.events.XAPlusScenarioInitialRequestEvent;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

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
    public void testUserRollbackBeforeRequestScenario() throws InterruptedException {
//        boolean status = finishedRequest(true, false);
//        assertFalse(status);
    }

    @Test
    public void testUserRollbackBeforeCommitScenario() throws InterruptedException {
//        boolean status = finishedRequest(false, true);
//        assertFalse(status);
    }

    @Test
    public void testFromSuperiorToSubordinatePrepareFailed() throws InterruptedException {
//        subordinateScenario.prepareException = true;
//        boolean status = finishedRequest(false, false);
//        assertFalse(status);
    }

    @Test
    public void testFromSubrodinateToSuperiorReadyFailed() throws InterruptedException {
//        superiorScenario.preparedException = true;
//        timedOutRequest();
    }

    @Test
    public void testFromSuperiorToSubordinateCommitFailed() throws InterruptedException {
//        subordinateScenario.commitException = true;
//        boolean status = finishedRequest(false, false);
//        assertFalse(status);
    }

    void timedOutRequest() throws InterruptedException {
        long value = initialRequest(false, false, false);
        XAPlusScenarioSuperiorFailedEvent scenarioFailedEvent = scenarioSuperiorFailedEvents
                .poll(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(scenarioFailedEvent);
        assertEquals(value, scenarioFailedEvent.getValue());
        assertTrue(scenarioFailedEvent.getException() instanceof XAPlusTimeoutException);
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
