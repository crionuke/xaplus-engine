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
    public void testSimple2pcScenario() throws InterruptedException {
        int value = 100;
        testDispatcher.dispatch(new XAPlusScenarioInitialRequestEvent(value));
        XAPlusScenarioFinishedEvent scenarioFinishedEvent = scenarioFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(scenarioFinishedEvent);
        assertEquals(value, scenarioFinishedEvent.getValue());
    }
}
