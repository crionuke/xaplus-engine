package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusDistributedTransactionFailedEvent;
import org.xaplus.engine.events.XAPlusDistributedTransactionFinishedEvent;
import org.xaplus.engine.events.XAPlusDistributedTransactionInitialRequestEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusDistributedTransactionScenarioTest extends XAPlusScenarioTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusDistributedTransactionScenarioTest.class);

    static private final String XA_PLUS_DISTRIBUTED = "distributed";

    private XAPlus distributedXAPlus;
    private DistributedTransactionBolt distributedTransactionBolt;
    private ConsumerBolt consumerBolt;

    @Before
    public void beforeTest() {
        createComponents();
        // Disable recovery by timer
        distributedXAPlus = new XAPlus(XA_PLUS_DISTRIBUTED, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S, 0);
        distributedXAPlus.construct();
        distributedTransactionBolt = new DistributedTransactionBolt(distributedXAPlus);
        distributedTransactionBolt.postConstruct();
        consumerBolt = new ConsumerBolt();
        consumerBolt.postConstruct();
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

    // Start XA transaction
    long startDistributedTransaction(boolean beforeCommitException) throws InterruptedException {
        long value = Math.round(100000 + Math.random() * 899999);
        testDispatcher.dispatch(new XAPlusDistributedTransactionInitialRequestEvent(value, beforeCommitException));
        return value;
    }

    // Distributed transaction implementation for test
    class DistributedTransactionBolt extends Bolt
            implements XAPlusDistributedTransactionInitialRequestEvent.Handler {

        XAPlus xaPlus;
        XAPlusEngine engine;

        DistributedTransactionBolt(XAPlus xaPlus) {
            super(XA_PLUS_DISTRIBUTED, QUEUE_SIZE);
            this.xaPlus = xaPlus;
            engine = xaPlus.engine;
            engine.register(database1, XA_RESOURCE_DATABASE_1);
            engine.register(database2, XA_RESOURCE_DATABASE_2);
            engine.setTLogDataSource(tlog);
        }

        @Override
        public void handleDistributedTransactionInitialRequest(XAPlusDistributedTransactionInitialRequestEvent event) throws InterruptedException {
            if (logger.isTraceEnabled()) {
                logger.trace("Handle {}", event);
            }
            long value = event.getValue();
            XAPlusFuture future;
            try {
                engine.begin();
                // Enlist and change jdbc resource1
                Connection connection1 = engine.enlistJdbc(XA_RESOURCE_DATABASE_1);
                try (PreparedStatement statement = connection1.prepareStatement(INSERT_VALUE)) {
                    statement.setLong(1, value);
                    statement.executeUpdate();
                }
                // Enlist and change jdbc resource2
                Connection connection2 = engine.enlistJdbc(XA_RESOURCE_DATABASE_2);
                try (PreparedStatement statement = connection2.prepareStatement(INSERT_VALUE)) {
                    statement.setLong(1, value);
                    statement.executeUpdate();
                }
                if (event.isBeforeCommitException()) {
                    throw new Exception("before_commit_exception");
                }
                // Commit distributed transaction
                future = engine.commit();
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Distributed transaction failed as {}", e.getMessage());
                }
                // Rollback distributed transaction
                future = engine.rollback();
            }
            // Wait result
            try {
                boolean status = future.getResult();
                logger.info("Distributed transaction finished, status={}", status);
                testDispatcher.dispatch(new XAPlusDistributedTransactionFinishedEvent(status, value));
            } catch (XAPlusCommitException commitException) {
                logger.info("Distributed transaction commit exception, {}", commitException.getMessage());
                testDispatcher.dispatch(new XAPlusDistributedTransactionFailedEvent(value, commitException));
            } catch (XAPlusRollbackException rollbackException) {
                logger.info("Distributed transaction rollback exception, {}", rollbackException.getMessage());
                testDispatcher.dispatch(new XAPlusDistributedTransactionFailedEvent(value, rollbackException));
            } catch (XAPlusTimeoutException timeoutException) {
                logger.info("Distributed transaction timeout exception, {}", timeoutException.getMessage());
                testDispatcher.dispatch(new XAPlusDistributedTransactionFailedEvent(value, timeoutException));
            }
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusDistributedTransactionInitialRequestEvent.class);
        }
    }

    // Bolt to collect events to queues
    class ConsumerBolt extends Bolt implements
            XAPlusDistributedTransactionFinishedEvent.Handler,
            XAPlusDistributedTransactionFailedEvent.Handler {

        BlockingQueue<XAPlusDistributedTransactionFinishedEvent> distributedTransactionFinishedEvents;
        BlockingQueue<XAPlusDistributedTransactionFailedEvent> distributedTransactionFailedEvents;

        ConsumerBolt() {
            super("consumer-bolt", QUEUE_SIZE);
            // Container for events
            distributedTransactionFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            distributedTransactionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleDistributedTransactionFinished(XAPlusDistributedTransactionFinishedEvent event) throws InterruptedException {
            distributedTransactionFinishedEvents.put(event);
        }

        @Override
        public void handleDistributedTransactionFailed(XAPlusDistributedTransactionFailedEvent event) throws InterruptedException {
            distributedTransactionFailedEvents.put(event);
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusDistributedTransactionFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusDistributedTransactionFailedEvent.class);
        }
    }
}
