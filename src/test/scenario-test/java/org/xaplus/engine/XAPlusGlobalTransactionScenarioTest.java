package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;
import org.xaplus.engine.events.recovery.XAPlusRecoveryFinishedEvent;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusGlobalTransactionScenarioTest extends XAPlusScenarioTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusGlobalTransactionScenarioTest.class);

    static private final String XA_PLUS_SUPERIOR = "superior";
    static private final String XA_PLUS_SUBORDINATE = "subordinate";

    private XAPlus superiorXAPlus;
    private XAPlus subordinateXAPLus;

    private XAPlusGlobalTransactionScenarioExceptions requestSuperiorExceptions;
    private XAPlusGlobalTransactionScenarioExceptions requestSubordinateExceptions;

    private XAPlusTestServer superiorServer;
    private XAPlusTestServer subordinateServer;

    private GlobalTransactionSuperiorBolt globalTransactionSuperiorBolt;
    private GlobalTransactionSubordinateBolt globalTransactionSubordinateBolt;

    private SuperiorInterceptorBolt superiorInterceptorBolt;
    private SubordinateInterceptorBolt subordinateInterceptorBolt;

    private ConsumerBolt consumerBolt;

    @Before
    public void beforeTest() {
        createComponents();

        superiorXAPlus = new XAPlus(XA_PLUS_SUPERIOR, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);
        superiorXAPlus.construct();
        subordinateXAPLus = new XAPlus(XA_PLUS_SUBORDINATE, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);
        subordinateXAPLus.construct();

        requestSuperiorExceptions = new XAPlusGlobalTransactionScenarioExceptions();
        requestSubordinateExceptions = new XAPlusGlobalTransactionScenarioExceptions();

        superiorServer = new XAPlusTestServer(requestSuperiorExceptions, superiorXAPlus.dispatcher);
        subordinateServer = new XAPlusTestServer(requestSubordinateExceptions, subordinateXAPLus.dispatcher);

        globalTransactionSuperiorBolt = new GlobalTransactionSuperiorBolt(superiorXAPlus);
        globalTransactionSuperiorBolt.postConstruct();
        globalTransactionSubordinateBolt = new GlobalTransactionSubordinateBolt(subordinateXAPLus);
        globalTransactionSubordinateBolt.postConstruct();

        superiorInterceptorBolt = new SuperiorInterceptorBolt();
        superiorInterceptorBolt.postConstruct();
        subordinateInterceptorBolt = new SubordinateInterceptorBolt();
        subordinateInterceptorBolt.postConstruct();

        consumerBolt = new ConsumerBolt();
        consumerBolt.postConstruct();
    }

    @Test
    public void testSuperiorCommitTransaction() throws InterruptedException {
        long value = startGlobalTransaction(false, false, false);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = consumerBolt.testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertTrue(event1.getStatus());
        // Check subordinate
        XAPlusTestSubordinateFinishedEvent event2 = consumerBolt.testSubordinateFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        assertTrue(event2.getStatus());
    }

    @Test
    public void testSuperiorCommitTransactionAndReportReadyStatusFromSubordinateToSuperiorFailed()
            throws InterruptedException {
        // Setup scenario
        requestSuperiorExceptions.readyException = true;
        long value = startGlobalTransaction(false, false, false);
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check superior
        XAPlusTestSuperiorFailedEvent event1 = consumerBolt.testSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Check subordinate
        XAPlusTestSubordinateFailedEvent event2 = consumerBolt.testSubordinateFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        // Recovery
        superiorXAPlus.engine.startRecovery();
        subordinateXAPLus.engine.startRecovery();
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000);
        XAPlusRecoveryFinishedEvent event3 = superiorInterceptorBolt.recoveryFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event3);
        XAPlusRecoveryFinishedEvent event4 = subordinateInterceptorBolt.recoveryFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event4);
    }

    @Test
    public void testSuperiorCommitRequestToSubordinateFailed() throws InterruptedException {
        // Setup scenario
        requestSubordinateExceptions.commitException = true;
        long value = startGlobalTransaction(false, false, false);
        // Check superior
        XAPlusTestSuperiorFailedEvent event1 = consumerBolt.testSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check subordinate
        XAPlusTestSubordinateFailedEvent event2 = consumerBolt.testSubordinateFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
    }

    @Test
    public void testSuperiorRollbackBeforeRequest() throws InterruptedException {
        long value = startGlobalTransaction(true, false, false);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = consumerBolt.testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    @Test
    public void testSuperiorRollbackBeforeCommit() throws InterruptedException {
        long value = startGlobalTransaction(false, true, false);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = consumerBolt.testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
    }

    @Test
    public void testSubordinateRollbackBeforeCommit() throws InterruptedException {
        long value = startGlobalTransaction(false, false, true);
        // Check superior
        XAPlusTestSuperiorFinishedEvent event1 = consumerBolt.testSuperiorFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        assertFalse(event1.getStatus());
        // Check subordinate
        XAPlusTestSubordinateFinishedEvent event2 = consumerBolt.testSubordinateFinishedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        assertFalse(event2.getStatus());
    }

    @Test
    public void testSubordinateRollbackBeforeCommitAndReportFailedStatusToSuperiorFailed() throws InterruptedException {
        // Setup scenario
        requestSuperiorExceptions.failedException = true;
        long value = startGlobalTransaction(false, false, true);
        // Wait timeout
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000 + POLL_TIMIOUT_MS);
        // Check superior
        XAPlusTestSuperiorFailedEvent event1 = consumerBolt.testSuperiorFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertEquals(value, event1.getValue());
        // Check subordinate
        XAPlusTestSubordinateFailedEvent event2 = consumerBolt.testSubordinateFailedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
        assertNotNull(event2);
        assertEquals(value, event2.getValue());
        // Superior recovery
        superiorXAPlus.engine.startRecovery();
        Thread.sleep(DEFAULT_TIMEOUT_S * 1000);
    }

    // Start XA+ transaction
    long startGlobalTransaction(boolean superiorBeforeRequestException,
                                boolean superiorBeforeCommitException,
                                boolean subordinateBeforeCommitException) throws InterruptedException {
        long value = Math.round(100000 + Math.random() * 899999);
        testDispatcher.dispatch(
                new XAPlusGlobalTransactionInitialRequestEvent(value, superiorBeforeRequestException,
                        superiorBeforeCommitException,
                        subordinateBeforeCommitException));
        return value;
    }

    // Superior side for global transaction test
    class GlobalTransactionSuperiorBolt extends Bolt
            implements XAPlusGlobalTransactionInitialRequestEvent.Handler {

        XAPlus xaPlus;
        XAPlusEngine engine;

        GlobalTransactionSuperiorBolt(XAPlus xaPlus) {
            super(XA_PLUS_SUPERIOR, QUEUE_SIZE);
            this.xaPlus = xaPlus;
            engine = xaPlus.engine;
            engine.register(database1, XA_RESOURCE_DATABASE_1);
            engine.setTLogDataSource(tlog);
            engine.register(subordinateServer, XA_PLUS_SUBORDINATE);
        }

        @Override
        public void handleGlobalTransactionInitialRequest(XAPlusGlobalTransactionInitialRequestEvent event) throws InterruptedException {
            if (logger.isTraceEnabled()) {
                logger.trace("Handle {}", event);
            }
            long value = event.getValue();
            XAPlusFuture future;
            try {
                engine.begin();
                // Enlist and change XA xaResource
                Connection connection = engine.enlistJdbc(XA_RESOURCE_DATABASE_1);
                try (PreparedStatement statement = connection.prepareStatement(INSERT_VALUE)) {
                    statement.setLong(1, value);
                    statement.executeUpdate();
                }
                // Enlist and call subordinate
                XAPlusXid branchXid = engine.enlistXAPlus(XA_PLUS_SUBORDINATE);
                if (event.isSuperiorBeforeRequestException()) {
                    throw new Exception("before_request_exception");
                }
                testDispatcher.dispatch(new XAPlusTestSubordinateRequestEvent(branchXid, value,
                        event.isSubordinateBeforeCommitException()));
                if (event.isSuperiorBeforeCommitException()) {
                    throw new Exception("before_commit_exception");
                }
                // Commit transaction
                future = engine.commit();
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Transaction failed as {}", e.getMessage());
                }
                // Rollback transaction
                future = engine.rollback();
            }
            // Wait result
            try {
                boolean status = future.get();
                logger.info("Superior side of transaction finished, status={}", status);
                testDispatcher.dispatch(new XAPlusTestSuperiorFinishedEvent(status, value));
            } catch (XAPlusCommitException commitException) {
                logger.info("Superior side had commit exception, {}", commitException.getMessage());
                testDispatcher.dispatch(new XAPlusTestSuperiorFailedEvent(value, commitException));
            } catch (XAPlusRollbackException rollbackException) {
                logger.info("Superior side had rollback exception, {}", rollbackException.getMessage());
                testDispatcher.dispatch(new XAPlusTestSuperiorFailedEvent(value, rollbackException));
            } catch (XAPlusTimeoutException timeoutException) {
                logger.info("Superior side had timeout exception, {}", timeoutException.getMessage());
                testDispatcher.dispatch(new XAPlusTestSuperiorFailedEvent(value, timeoutException));
            }
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusGlobalTransactionInitialRequestEvent.class);
        }
    }

    // Subordinate side for global transaction test
    class GlobalTransactionSubordinateBolt extends Bolt implements XAPlusTestSubordinateRequestEvent.Handler {

        XAPlus xaPlus;
        XAPlusEngine engine;

        GlobalTransactionSubordinateBolt(XAPlus xaPlus) {
            super(XA_PLUS_SUBORDINATE, QUEUE_SIZE);
            this.xaPlus = xaPlus;
            engine = xaPlus.engine;
            engine.register(database2, XA_RESOURCE_DATABASE_2);
            engine.setTLogDataSource(tlog);
            engine.register(superiorServer, XA_PLUS_SUPERIOR);
        }

        @Override
        public void handleTestSubordinateRequest(XAPlusTestSubordinateRequestEvent event) throws InterruptedException {
            if (logger.isTraceEnabled()) {
                logger.trace("Handle {}", event);
            }
            XAPlusXid xid = event.getXid();
            long value = event.getValue();
            XAPlusFuture future;
            try {
                engine.join(xid);
                // Enlist and change XA xaResource
                Connection connection = engine.enlistJdbc(XA_RESOURCE_DATABASE_2);
                try (PreparedStatement statement = connection.prepareStatement(INSERT_VALUE)) {
                    statement.setLong(1, value);
                    statement.executeUpdate();
                }
                if (event.isBeforeCommitException()) {
                    throw new Exception("before_commit_exception");
                }
                // Commit transaction
                future = engine.commit();
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Transaction failed as {}", e.getMessage());
                }
                // Rollback transaction
                future = engine.rollback();
            }
            // Wait result
            try {
                boolean status = future.get();
                logger.info("Subordinate side of transaction finished, status={}", status);
                testDispatcher.dispatch(new XAPlusTestSubordinateFinishedEvent(status, value));
            } catch (XAPlusCommitException commitException) {
                logger.info("Subordinate side had commit exception, {}", commitException.getMessage());
                testDispatcher.dispatch(new XAPlusTestSubordinateFailedEvent(value, commitException));
            } catch (XAPlusRollbackException rollbackException) {
                logger.info("Subordinate side had rollback exception, {}", rollbackException.getMessage());
                testDispatcher.dispatch(new XAPlusTestSubordinateFailedEvent(value, rollbackException));
            } catch (XAPlusTimeoutException timeoutException) {
                logger.info("Subordinate side had timeout exception, {}", timeoutException.getMessage());
                testDispatcher.dispatch(new XAPlusTestSubordinateFailedEvent(value, timeoutException));
            }
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusTestSubordinateRequestEvent.class);
        }
    }

    class SuperiorInterceptorBolt extends Bolt implements
            XAPlusRecoveryFinishedEvent.Handler {

        BlockingQueue<XAPlusRecoveryFinishedEvent> recoveryFinishedEvents;

        SuperiorInterceptorBolt() {
            super("superior-interceptor-bolt", QUEUE_SIZE);
            // Container for events
            recoveryFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleRecoveryFinished(XAPlusRecoveryFinishedEvent event) throws InterruptedException {
            recoveryFinishedEvents.put(event);
        }

        void postConstruct() {
            testThreadPool.execute(this);
            superiorXAPlus.dispatcher.subscribe(this, XAPlusRecoveryFinishedEvent.class);
        }
    }

    class SubordinateInterceptorBolt extends Bolt implements
            XAPlusRecoveryFinishedEvent.Handler {

        BlockingQueue<XAPlusRecoveryFinishedEvent> recoveryFinishedEvents;

        SubordinateInterceptorBolt() {
            super("subordinate-interceptor-bolt", QUEUE_SIZE);
            // Container for events
            recoveryFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleRecoveryFinished(XAPlusRecoveryFinishedEvent event) throws InterruptedException {
            recoveryFinishedEvents.put(event);
        }

        void postConstruct() {
            testThreadPool.execute(this);
            subordinateXAPLus.dispatcher.subscribe(this, XAPlusRecoveryFinishedEvent.class);
        }
    }

    // Bolt to collect events to queues
    class ConsumerBolt extends Bolt implements
            XAPlusTestSuperiorFinishedEvent.Handler,
            XAPlusTestSuperiorFailedEvent.Handler,
            XAPlusTestSubordinateFinishedEvent.Handler,
            XAPlusTestSubordinateFailedEvent.Handler {

        BlockingQueue<XAPlusTestSuperiorFinishedEvent> testSuperiorFinishedEvents;
        BlockingQueue<XAPlusTestSuperiorFailedEvent> testSuperiorFailedEvents;
        BlockingQueue<XAPlusTestSubordinateFinishedEvent> testSubordinateFinishedEvents;
        BlockingQueue<XAPlusTestSubordinateFailedEvent> testSubordinateFailedEvents;

        ConsumerBolt() {
            super("consumer-bolt", QUEUE_SIZE);
            // Container for events
            testSuperiorFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            testSuperiorFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            testSubordinateFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            testSubordinateFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleTestSuperiorFinished(XAPlusTestSuperiorFinishedEvent event) throws InterruptedException {
            testSuperiorFinishedEvents.put(event);
        }

        @Override
        public void handleTestSuperiorFailed(XAPlusTestSuperiorFailedEvent event) throws InterruptedException {
            testSuperiorFailedEvents.put(event);
        }

        @Override
        public void handleTestSubordinateFinished(XAPlusTestSubordinateFinishedEvent event) throws InterruptedException {
            testSubordinateFinishedEvents.put(event);
        }

        @Override
        public void handleTestSubordinateFailed(XAPlusTestSubordinateFailedEvent event) throws InterruptedException {
            testSubordinateFailedEvents.put(event);
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusTestSuperiorFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusTestSuperiorFailedEvent.class);
            testDispatcher.subscribe(this, XAPlusTestSubordinateFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusTestSubordinateFailedEvent.class);
        }
    }
}
