package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusDistributedTransactionFinishedEvent;

import java.util.concurrent.TimeUnit;

public class XAPlusDistributedTransactionScenarioTest extends XAPlusScenarioTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusDistributedTransactionScenarioTest.class);

    @Before
    public void beforeTest() {
        createComponents();
        construct();
    }

    @Test
    public void testCommitDistributedTransaction() throws InterruptedException {
        long value = startDistributedTransaction(false);
        // Check transaction
        XAPlusDistributedTransactionFinishedEvent event1 = consumerBolt.distributedTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
    }

    @Test
    public void testRollbackDistributedTransaction() throws InterruptedException {
        long value = startDistributedTransaction(true);
        // Check transaction
        XAPlusDistributedTransactionFinishedEvent event1 = consumerBolt.distributedTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }
}
