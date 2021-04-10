package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;

import java.util.concurrent.TimeUnit;

public class XAPlusLocalTransactionScenarioTest extends XAPlusScenarioTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusLocalTransactionScenarioTest.class);

    @Before
    public void beforeTest() {
        createComponents();
        construct();
    }

    @Test
    public void testCommitLocalTransaction() throws InterruptedException {
        long value = startLocalTransaction();
        // Check transaction
        XAPlusLocalTransactionFinishedEvent event1 = consumerBolt.localTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
    }
}
