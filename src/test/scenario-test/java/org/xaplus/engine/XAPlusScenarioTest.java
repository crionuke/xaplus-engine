package org.xaplus.engine;

import com.crionuke.bolts.Bolt;
import com.crionuke.bolts.Dispatcher;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.postgresql.xa.PGXADataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.XAPlusScenarioFailedEvent;
import org.xaplus.engine.events.XAPlusScenarioFinishedEvent;
import org.xaplus.engine.events.XAPlusScenarioInitialRequestEvent;
import org.xaplus.engine.events.XAPlusScenarioSubordinateRequestEvent;
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
    static final int QUEUE_SIZE = 128;
    static final int DEFAULT_TIMEOUT_S = 10;
    static final int POLL_TIMIOUT_MS = 4000;
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

    XAPlusTestScenario superiorScenario;
    XAPlusTestScenario subordinateScenario;

    XAPlusTestServer superiorServer;
    XAPlusTestServer subordinateServer;

    BlockingQueue<XAPlusScenarioFinishedEvent> scenarioFinishedEvents;
    BlockingQueue<XAPlusScenarioFailedEvent> scenarioFailedEvents;

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

        superiorScenario = new XAPlusTestScenario();
        subordinateScenario = new XAPlusTestScenario();

        superiorServer = new XAPlusTestServer(superiorScenario, superiorXAPlus.dispatcher);
        subordinateServer = new XAPlusTestServer(subordinateScenario, subordinateXAPLus.dispatcher);

        scenarioFinishedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        scenarioFailedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

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
            engine = xaPlus.getEngine();
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
                if (event.isBeforeRequestException()) {
                    throw new Exception("fail");
                }
                testDispatcher.dispatch(new XAPlusScenarioSubordinateRequestEvent(branchXid, value));
                if (event.isBeforeCommitException()) {
                    throw new Exception("fail");
                }
                // Commit XA+ transaction
                future = engine.commit();
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Transaction failed as {}", e.getMessage());
                }
                // Rollback XA+ transaction
                future = engine.rollback();
            }
            // Wait result
            try {
                boolean result = future.get();
                testDispatcher.dispatch(new XAPlusScenarioFinishedEvent(value));
            } catch (XAPlusCommitException commitException) {
                testDispatcher.dispatch(new XAPlusScenarioFailedEvent(value, commitException));
            } catch (XAPlusRollbackException rollbackException) {
                testDispatcher.dispatch(new XAPlusScenarioFailedEvent(value, rollbackException));
            } catch (XAPlusTimeoutException timeoutException) {
                testDispatcher.dispatch(new XAPlusScenarioFailedEvent(value, timeoutException));
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
            engine = xaPlus.getEngine();
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
                // Commit branch of XA+ transaction
                future = engine.commit();
            } catch (Exception e) {
                if (logger.isWarnEnabled()) {
                    logger.warn("Transaction failed as {}", e);
                }
                // Rollback branch of XA+ transaction
                future = engine.rollback();
            }
            // Wait result
            try {
                boolean result = future.get();
                assertEquals(true, result);
            } catch (XAPlusCommitException commitException) {
                fail(commitException.getMessage());
            } catch (XAPlusRollbackException rollbackException) {
                fail(rollbackException.getMessage());
            } catch (XAPlusTimeoutException timeoutException) {
                fail(timeoutException.getMessage());
            }
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusScenarioSubordinateRequestEvent.class);
        }
    }

    class Controller extends Bolt implements
            XAPlusScenarioFinishedEvent.Handler,
            XAPlusScenarioFailedEvent.Handler {

        Controller() {
            super("controller", QUEUE_SIZE);
        }

        @Override
        public void handleScenarioFinished(XAPlusScenarioFinishedEvent event) throws InterruptedException {
            scenarioFinishedEvents.put(event);
        }

        @Override
        public void handleScenarioFinished(XAPlusScenarioFailedEvent event) throws InterruptedException {
            scenarioFailedEvents.put(event);
        }

        void postConstruct() {
            testThreadPool.execute(this);
            testDispatcher.subscribe(this, XAPlusScenarioFinishedEvent.class);
            testDispatcher.subscribe(this, XAPlusScenarioFailedEvent.class);
        }
    }
}
