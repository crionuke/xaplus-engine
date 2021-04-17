package org.xaplus.engine;

import com.crionuke.bolts.Dispatcher;
import org.apache.tomcat.jdbc.pool.DataSource;
import org.junit.Assert;
import org.postgresql.xa.PGXADataSource;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class XAPlusScenarioTest extends Assert {
    static protected final int QUEUE_SIZE = 128;
    static protected final int DEFAULT_TIMEOUT_S = 4;
    static protected final int POLL_TIMIOUT_MS = 2000;
    static protected final String INSERT_VALUE = "INSERT INTO test (t_value) VALUES(?)";
    static protected final String XA_RESOURCE_DATABASE_1 = "database1";
    static protected final String XA_RESOURCE_DATABASE_2 = "database2";

    protected DataSource tlog;
    protected PGXADataSource database1;
    protected PGXADataSource database2;

    protected ExecutorService testThreadPool;
    protected Dispatcher testDispatcher;

    void createComponents() {
        // Datasources to tests
        tlog = createTLog();
        database1 = createDatabase1();
        database2 = createDatabase2();

        testThreadPool = Executors.newFixedThreadPool(16);
        testDispatcher = new Dispatcher();
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
}
