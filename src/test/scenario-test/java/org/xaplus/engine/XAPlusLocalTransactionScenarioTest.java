package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusLocalTransactionFailedEvent;
import org.xaplus.engine.events.XAPlusLocalTransactionFinishedEvent;
import org.xaplus.engine.events.XAPlusLocalTransactionInitialRequestEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusLocalTransactionScenarioTest extends XAPlusScenarioTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusLocalTransactionScenarioTest.class);

    static private final String XA_PLUS_LOCAL = "local";

    protected XAPlus localXAPlus;

    private LocalTransactionBolt localTransactionBolt;
    private ConsumerBolt consumerBolt;

    @Before
    public void beforeTest() {
        createComponents();

        localXAPlus = new XAPlus(XA_PLUS_LOCAL, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);
        localXAPlus.construct();
        localTransactionBolt = new LocalTransactionBolt(localXAPlus);
        localTransactionBolt.postConstruct();
        consumerBolt = new ConsumerBolt();
        consumerBolt.postConstruct();
    }

    @Test
    public void testCommitLocalTransaction() throws InterruptedException {
        long value = startLocalTransaction(false);
        // Check transaction
        XAPlusLocalTransactionFinishedEvent event1 = consumerBolt.localTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
    }

    @Test
    public void testRollbackLocalTransaction() throws InterruptedException {
        long value = startLocalTransaction(true);
        // Check transaction
        XAPlusLocalTransactionFinishedEvent event1 = consumerBolt.localTransactionFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    long startLocalTransaction(boolean beforeCommitException) throws InterruptedException {
        long value = Math.round(100000 + Math.random() * 899999);
        testDispatcher.dispatch(new XAPlusLocalTransactionInitialRequestEvent(value, beforeCommitException));
        return value;
    }

    // Local transaction implementation for test
    class LocalTransactionBolt extends Bolt
            implements XAPlusLocalTransactionInitialRequestEvent.Handler {

        XAPlus xaPlus;
        XAPlusEngine engine;

        LocalTransactionBolt(XAPlus xaPlus) {
            super(XA_PLUS_LOCAL, QUEUE_SIZE);
            this.xaPlus = xaPlus;
            engine = xaPlus.engine;
            engine.register(database1, XA_RESOURCE_DATABASE_1);
            engine.setTLogDataSource(tlog);
        }

        @Override
        public void handleLocalTransactionInitialRequest(XAPlusLocalTransactionInitialRequestEvent event) throws InterruptedException {
            if (logger.isTraceEnabled()) {
                logger.trace("Handle {}", event);
            }
            long value = event.getValue();
            XAPlusFuture future;
            try {
                engine.begin();
                // Enlist and change jdbc resource
                Connection connection = engine.enlistJdbc(XA_RESOURCE_DATABASE_1);
                try (PreparedStatement statement = connection.prepareStatement(INSERT_VALUE)) {
                    statement.setLong(1, value);
                    statement.executeUpdate();
                }
                if (event.isBeforeCommitException()) {
                    throw new Exception("before_commit_exception");
                }
                // Commit local transaction
                future = engine.commit();
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Local transaction failed as {}", e.getMessage());
                }
                // Rollback local transaction
                future = engine.rollback();
            }
            // Wait result
            try {
                boolean status = future.getResult();
                logger.info("Local transaction finished, status={}", status);
                testDispatcher.dispatch(new XAPlusLocalTransactionFinishedEvent(status, value));
            } catch (XAPlusCommitException commitException) {
                logger.info("Local transaction commit exception, {}", commitException.getMessage());
                testDispatcher.dispatch(new XAPlusLocalTransactionFailedEvent(value, commitException));
            } catch (XAPlusRollbackException rollbackException) {
                logger.info("Local transaction rollback exception, {}", rollbackException.getMessage());
                testDispatcher.dispatch(new XAPlusLocalTransactionFailedEvent(value, rollbackException));
            } catch (XAPlusTimeoutException timeoutException) {
                logger.info("Local transaction timeout exception, {}", timeoutException.getMessage());
                testDispatcher.dispatch(new XAPlusLocalTransactionFailedEvent(value, timeoutException));
            }
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusLocalTransactionInitialRequestEvent.class);
        }
    }

    // Bolt to collect events to queues
    class ConsumerBolt extends Bolt implements
            XAPlusLocalTransactionFinishedEvent.Handler,
            XAPlusLocalTransactionFailedEvent.Handler {

        BlockingQueue<XAPlusLocalTransactionFinishedEvent> localTransactionFinishedEvents;
        BlockingQueue<XAPlusLocalTransactionFailedEvent> localTransactionFailedEvents;

        ConsumerBolt() {
            super("consumer-bolt", QUEUE_SIZE);
            // Container for events
            localTransactionFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            localTransactionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleLocalTransactionFinished(XAPlusLocalTransactionFinishedEvent event) throws InterruptedException {
            localTransactionFinishedEvents.put(event);
        }

        @Override
        public void handleLocalTransactionFailed(XAPlusLocalTransactionFailedEvent event) throws InterruptedException {
            localTransactionFailedEvents.put(event);
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusLocalTransactionFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusLocalTransactionFailedEvent.class);
        }
    }
}
