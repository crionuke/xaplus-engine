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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

public class XAPlusScenarioTest extends Assert {
    static final int QUEUE_SIZE = 128;
    static final int DEFAULT_TIMEOUT_S = 4;
    static final int POLL_TIMIOUT_MS = 2000;
    static final String INSERT_VALUE = "INSERT INTO test (t_value) VALUES(?)";
    static final String XA_RESOURCE_DATABASE_1 = "database1";
    static final String XA_RESOURCE_DATABASE_2 = "database2";
    static final String XA_PLUS_SUPERIOR = "superior";
    static final String XA_PLUS_SUBORDINATE = "subordinate";
    static private final Logger logger = LoggerFactory.getLogger(XAPlusScenarioTest.class);
    DataSource tlog;
    PGXADataSource database1;
    PGXADataSource database2;

    XAPlus superiorXAPlus;
    XAPlus subordinateXAPLus;

    XAPlusScenarioExceptions requestSuperiorExceptions;
    XAPlusScenarioExceptions subordinateScenarioExceptions;

    XAPlusTestServer superiorServer;
    XAPlusTestServer subordinateServer;

    BlockingQueue<XAPlusScenarioSuperiorFinishedEvent> scenarioSuperiorFinishedEvents;
    BlockingQueue<XAPlusScenarioSuperiorFailedEvent> scenarioSuperiorFailedEvents;
    BlockingQueue<XAPlusScenarioSubordinateFinishedEvent> scenarioSubordinateFinishedEvents;
    BlockingQueue<XAPlusScenarioSubordinateFailedEvent> scenarioSubordinateFailedEvents;

    ExecutorService testThreadPool;
    Dispatcher testDispatcher;
    Controller controllerBolt;
    Superior superiorBolt;
    Subordinate subordinateBolt;

    void createComponents() {
        tlog = createTLog();
        database1 = createDatabase1();
        database2 = createDatabase2();

        superiorXAPlus = new XAPlus(XA_PLUS_SUPERIOR, DEFAULT_TIMEOUT_S);
        subordinateXAPLus = new XAPlus(XA_PLUS_SUBORDINATE, DEFAULT_TIMEOUT_S);

        requestSuperiorExceptions = new XAPlusScenarioExceptions();
        subordinateScenarioExceptions = new XAPlusScenarioExceptions();

        superiorServer = new XAPlusTestServer(requestSuperiorExceptions, superiorXAPlus.dispatcher);
        subordinateServer = new XAPlusTestServer(subordinateScenarioExceptions, subordinateXAPLus.dispatcher);

        scenarioSuperiorFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        scenarioSuperiorFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        scenarioSubordinateFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        scenarioSubordinateFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        testThreadPool = Executors.newFixedThreadPool(16);
        testDispatcher = new Dispatcher();

        controllerBolt = new Controller();
        controllerBolt.postConstruct();

        superiorBolt = new Superior(superiorXAPlus);
        superiorBolt.postConstruct();
        subordinateBolt = new Subordinate(subordinateXAPLus);
        subordinateBolt.postConstruct();
    }

    void start() {
        superiorXAPlus.start();
        subordinateXAPLus.start();
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

    class Superior extends Bolt
            implements XAPlusScenarioInitialRequestEvent.Handler {

        XAPlus xaPlus;
        XAPlusEngine engine;

        Superior(XAPlus xaPlus) {
            super(XA_PLUS_SUPERIOR, QUEUE_SIZE);
            this.xaPlus = xaPlus;
            engine = xaPlus.engine;
            engine.register(database1, XA_RESOURCE_DATABASE_1);
            engine.setTLogDataSource(tlog);
            engine.register(subordinateServer, XA_PLUS_SUBORDINATE);
        }

        @Override
        public void handleScenarioInitialRequest(XAPlusScenarioInitialRequestEvent event) throws InterruptedException {
            if (logger.isTraceEnabled()) {
                logger.trace("Handle {}", event);
            }
            long value = event.getValue();
            XAPlusFuture future;
            List<XAPlusXid> usedXids = new ArrayList<>();
            try {
                engine.begin();
                // Enlist and change XA xaResource
                Connection connection = engine.enlistJdbc(XA_RESOURCE_DATABASE_1);
                try (PreparedStatement statement = connection.prepareStatement(INSERT_VALUE)) {
                    statement.setLong(1, value);
                    statement.executeUpdate();
                }
                // Enlist and call subordinate
                XAPlusXid branchXid = engine.createXAPlusXid(XA_PLUS_SUBORDINATE);
                if (event.isSuperiorBeforeRequestException()) {
                    throw new Exception("before request exception");
                }
                testDispatcher.dispatch(new XAPlusScenarioSubordinateRequestEvent(branchXid, value,
                        event.isSubordinateBeforeCommitException()));
                usedXids.add(branchXid);
                if (event.isSuperiorBeforeCommitException()) {
                    throw new Exception("before commit exception");
                }
                // Commit transaction
                future = engine.commit(usedXids);
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Transaction failed as {}", e.getMessage());
                }
                // Rollback transaction
                future = engine.rollback(usedXids);
            }
            // Wait result
            try {
                boolean status = future.get();
                logger.info("Superior side of transaction finished, status={}", status);
                testDispatcher.dispatch(new XAPlusScenarioSuperiorFinishedEvent(status, value));
            } catch (XAPlusCommitException commitException) {
                logger.info("Superior side had commit exception, {}", commitException.getMessage());
                testDispatcher.dispatch(new XAPlusScenarioSuperiorFailedEvent(value, commitException));
            } catch (XAPlusRollbackException rollbackException) {
                logger.info("Superior side had rollback exception, {}", rollbackException.getMessage());
                testDispatcher.dispatch(new XAPlusScenarioSuperiorFailedEvent(value, rollbackException));
            } catch (XAPlusTimeoutException timeoutException) {
                logger.info("Superior side had timeout exception, {}", timeoutException.getMessage());
                testDispatcher.dispatch(new XAPlusScenarioSuperiorFailedEvent(value, timeoutException));
            }
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusScenarioInitialRequestEvent.class);
        }
    }

    class Subordinate extends Bolt implements XAPlusScenarioSubordinateRequestEvent.Handler {

        XAPlus xaPlus;
        XAPlusEngine engine;

        Subordinate(XAPlus xaPlus) {
            super(XA_PLUS_SUBORDINATE, QUEUE_SIZE);
            this.xaPlus = xaPlus;
            engine = xaPlus.engine;
            engine.register(database2, XA_RESOURCE_DATABASE_2);
            engine.setTLogDataSource(tlog);
            engine.register(superiorServer, XA_PLUS_SUPERIOR);
        }

        @Override
        public void handleScenarioSubordinateRequest(XAPlusScenarioSubordinateRequestEvent event) throws InterruptedException {
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
                    throw new Exception("before commit exception");
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
                testDispatcher.dispatch(new XAPlusScenarioSubordinateFinishedEvent(status, value));
            } catch (XAPlusCommitException commitException) {
                logger.info("Subordinate side had commit exception, {}", commitException.getMessage());
                testDispatcher.dispatch(new XAPlusScenarioSubordinateFailedEvent(value, commitException));
            } catch (XAPlusRollbackException rollbackException) {
                logger.info("Subordinate side had rollback exception, {}", rollbackException.getMessage());
                testDispatcher.dispatch(new XAPlusScenarioSubordinateFailedEvent(value, rollbackException));
            } catch (XAPlusTimeoutException timeoutException) {
                logger.info("Subordinate side had timeout exception, {}", timeoutException.getMessage());
                testDispatcher.dispatch(new XAPlusScenarioSubordinateFailedEvent(value, timeoutException));
            }
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusScenarioSubordinateRequestEvent.class);
        }
    }

    class Controller extends Bolt implements
            XAPlusScenarioSuperiorFinishedEvent.Handler,
            XAPlusScenarioSuperiorFailedEvent.Handler,
            XAPlusScenarioSubordinateFinishedEvent.Handler,
            XAPlusScenarioSubordinateFailedEvent.Handler {

        Controller() {
            super("controller", QUEUE_SIZE);
        }

        @Override
        public void handleScenarioSuperiorFinished(XAPlusScenarioSuperiorFinishedEvent event) throws InterruptedException {
            scenarioSuperiorFinishedEvents.put(event);
        }

        @Override
        public void handleScenarioSuperiorFailed(XAPlusScenarioSuperiorFailedEvent event) throws InterruptedException {
            scenarioSuperiorFailedEvents.put(event);
        }

        @Override
        public void handleScenarioSubordinateFinished(XAPlusScenarioSubordinateFinishedEvent event) throws InterruptedException {
            scenarioSubordinateFinishedEvents.put(event);
        }

        @Override
        public void handleScenarioSubordinateFailed(XAPlusScenarioSubordinateFailedEvent event) throws InterruptedException {
            scenarioSubordinateFailedEvents.put(event);
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusScenarioSuperiorFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusScenarioSuperiorFailedEvent.class);
            testDispatcher.subscribe(this, XAPlusScenarioSubordinateFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusScenarioSubordinateFailedEvent.class);
        }
    }
}
