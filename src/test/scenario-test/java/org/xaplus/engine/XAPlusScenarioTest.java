package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import com.crionuke.bolts.Dispatcher;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.postgresql.xa.PGXADataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.*;
import org.xaplus.engine.exceptions.XAPlusCommitException;
import org.xaplus.engine.exceptions.XAPlusRollbackException;
import org.xaplus.engine.exceptions.XAPlusTimeoutException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class XAPlusScenarioTest extends Assert {
    static protected final int QUEUE_SIZE = 128;
    static protected final int DEFAULT_TIMEOUT_S = 4;
    static protected final int POLL_TIMIOUT_MS = 2000;
    static protected final String INSERT_VALUE = "INSERT INTO test (t_value) VALUES(?)";
    static protected final String XA_RESOURCE_DATABASE_1 = "database1";
    static protected final String XA_RESOURCE_DATABASE_2 = "database2";
    static protected final String XA_PLUS_LOCAL = "local";
    static protected final String XA_PLUS_DISTRIBUTED = "distributed";
    static protected final String XA_PLUS_SUPERIOR = "superior";
    static protected final String XA_PLUS_SUBORDINATE = "subordinate";
    static private final Logger logger = LoggerFactory.getLogger(XAPlusScenarioTest.class);
    protected DataSource tlog;
    protected PGXADataSource database1;
    protected PGXADataSource database2;

    protected XAPlus localXAPlus;
    protected XAPlus distributedXAPlus;
    protected XAPlus superiorXAPlus;
    protected XAPlus subordinateXAPLus;

    protected XAPlusScenarioExceptions requestSuperiorExceptions;
    protected XAPlusScenarioExceptions requestSubordinateExceptions;

    protected XAPlusTestServer superiorServer;
    protected XAPlusTestServer subordinateServer;

    protected ExecutorService testThreadPool;
    protected Dispatcher testDispatcher;

    protected LocalTransactionBolt localTransactionBoltBolt;
    protected DistributedTransactionBolt distributedTransactionBoltBolt;
    protected GlobalTransactionSuperiorBolt globalTransactionSuperiorBolt;
    protected GlobalTransactionSubordinateBolt globalTransactionSubordinateBolt;
    protected ConsumerBolt consumerBolt;

    void createComponents() {
        // Datasources to tests
        tlog = createTLog();
        database1 = createDatabase1();
        database2 = createDatabase2();

        // XAPlus components to bolts
        localXAPlus = new XAPlus(XA_PLUS_LOCAL, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);
        distributedXAPlus = new XAPlus(XA_PLUS_DISTRIBUTED, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);
        superiorXAPlus = new XAPlus(XA_PLUS_SUPERIOR, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);
        subordinateXAPLus = new XAPlus(XA_PLUS_SUBORDINATE, DEFAULT_TIMEOUT_S, DEFAULT_TIMEOUT_S);

        requestSuperiorExceptions = new XAPlusScenarioExceptions();
        requestSubordinateExceptions = new XAPlusScenarioExceptions();

        superiorServer = new XAPlusTestServer(requestSuperiorExceptions, superiorXAPlus.dispatcher);
        subordinateServer = new XAPlusTestServer(requestSubordinateExceptions, subordinateXAPLus.dispatcher);

        testThreadPool = Executors.newFixedThreadPool(16);
        testDispatcher = new Dispatcher();

        // Create and construct bolts
        localTransactionBoltBolt = new LocalTransactionBolt(localXAPlus);
        localTransactionBoltBolt.postConstruct();
        distributedTransactionBoltBolt = new DistributedTransactionBolt(distributedXAPlus);
        distributedTransactionBoltBolt.postConstruct();
        globalTransactionSuperiorBolt = new GlobalTransactionSuperiorBolt(superiorXAPlus);
        globalTransactionSuperiorBolt.postConstruct();
        globalTransactionSubordinateBolt = new GlobalTransactionSubordinateBolt(subordinateXAPLus);
        globalTransactionSubordinateBolt.postConstruct();
        consumerBolt = new ConsumerBolt();
        consumerBolt.postConstruct();
    }

    void construct() {
        // Construct components
        localXAPlus.construct();
        distributedXAPlus.construct();
        superiorXAPlus.construct();
        subordinateXAPLus.construct();
    }

    long startLocalTransaction(boolean beforeCommitException) throws InterruptedException {
        long value = Math.round(100000 + Math.random() * 899999);
        testDispatcher.dispatch(new XAPlusLocalTransactionInitialRequestEvent(value, beforeCommitException));
        return value;
    }

    // Start XA transaction
    long startDistributedTransaction(boolean beforeCommitException) throws InterruptedException {
        long value = Math.round(100000 + Math.random() * 899999);
        testDispatcher.dispatch(new XAPlusDistributedTransactionInitialRequestEvent(value, beforeCommitException));
        return value;
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

    DataSource createTLog() {
        DataSource dataSource = new DataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:10000/tlog");
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUsername("tlog");
        dataSource.setPassword("qwe123");
        return dataSource;
    }

    PGXADataSource createDatabase1() {
        PGXADataSource dataSource = new PGXADataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:10001/test");
        dataSource.setUser("test");
        dataSource.setPassword("qwe123");
        return dataSource;
    }

    PGXADataSource createDatabase2() {
        PGXADataSource dataSource = new PGXADataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:10002/test");
        dataSource.setUser("test");
        dataSource.setPassword("qwe123");
        return dataSource;
    }

    // Simple local transaction implementation for test
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
                boolean status = future.get();
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

    // DistributedTransactionBolt transaction implementation for test
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
                boolean status = future.get();
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

    // Bolt to collect events to queues
    class ConsumerBolt extends Bolt implements
            XAPlusLocalTransactionFinishedEvent.Handler,
            XAPlusLocalTransactionFailedEvent.Handler,
            XAPlusDistributedTransactionFinishedEvent.Handler,
            XAPlusDistributedTransactionFailedEvent.Handler,
            XAPlusTestSuperiorFinishedEvent.Handler,
            XAPlusTestSuperiorFailedEvent.Handler,
            XAPlusTestSubordinateFinishedEvent.Handler,
            XAPlusTestSubordinateFailedEvent.Handler {

        BlockingQueue<XAPlusLocalTransactionFinishedEvent> localTransactionFinishedEvents;
        BlockingQueue<XAPlusLocalTransactionFailedEvent> localTransactionFailedEvents;
        BlockingQueue<XAPlusDistributedTransactionFinishedEvent> distributedTransactionFinishedEvents;
        BlockingQueue<XAPlusDistributedTransactionFailedEvent> distributedTransactionFailedEvents;
        BlockingQueue<XAPlusTestSuperiorFinishedEvent> testSuperiorFinishedEvents;
        BlockingQueue<XAPlusTestSuperiorFailedEvent> testSuperiorFailedEvents;
        BlockingQueue<XAPlusTestSubordinateFinishedEvent> testSubordinateFinishedEvents;
        BlockingQueue<XAPlusTestSubordinateFailedEvent> testSubordinateFailedEvents;

        ConsumerBolt() {
            super("consumer-bolt", QUEUE_SIZE);
            // Container for events
            localTransactionFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            localTransactionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            distributedTransactionFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            distributedTransactionFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            testSuperiorFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            testSuperiorFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            testSubordinateFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
            testSubordinateFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        }

        @Override
        public void handleLocalTransactionFinished(XAPlusLocalTransactionFinishedEvent event) throws InterruptedException {
            localTransactionFinishedEvents.put(event);
        }

        @Override
        public void handleLocalTransactionFailed(XAPlusLocalTransactionFailedEvent event) throws InterruptedException {
            localTransactionFailedEvents.put(event);
        }

        @Override
        public void handleDistributedTransactionFinished(XAPlusDistributedTransactionFinishedEvent event) throws InterruptedException {
            distributedTransactionFinishedEvents.put(event);
        }

        @Override
        public void handleDistributedTransactionFailed(XAPlusDistributedTransactionFailedEvent event) throws InterruptedException {
            distributedTransactionFailedEvents.put(event);
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
            testDispatcher.subscribe(this, XAPlusLocalTransactionFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusLocalTransactionFailedEvent.class);
            testDispatcher.subscribe(this, XAPlusDistributedTransactionFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusDistributedTransactionFailedEvent.class);
            testDispatcher.subscribe(this, XAPlusTestSuperiorFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusTestSuperiorFailedEvent.class);
            testDispatcher.subscribe(this, XAPlusTestSubordinateFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusTestSubordinateFailedEvent.class);
        }
    }
}
