package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusScenarioFinishedEvent;
import org.xaplus.engine.events.XAPlusScenarioInitialRequestEvent;

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
        finishedRequest(false, false);
    }

    @Test
    public void testUserRollbackBeforeRequestScenario() throws InterruptedException {
        finishedRequest(true, false);
    }

    @Test
    public void testUserRollbackBeforeCommitScenario() throws InterruptedException {
        finishedRequest(false, true);
    }

    @Test
    public void testFromSuperiorToSubordinatePrepareFailed() throws InterruptedException {
        subordinateScenario.prepareException = false;
    }

    void finishedRequest(boolean beforeRequestException, boolean beforeCommitException) throws InterruptedException {
        long value = Math.round(100000 + Math.random() * 899999);
        testDispatcher.dispatch(
                new XAPlusScenarioInitialRequestEvent(value, beforeRequestException, beforeCommitException));
        XAPlusScenarioFinishedEvent scenarioFinishedEvent = scenarioFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(scenarioFinishedEvent);
        assertEquals(value, scenarioFinishedEvent.getValue());
    }
}
