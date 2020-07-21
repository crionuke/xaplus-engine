package com.crionuke.xaplus;

import com.crionuke.bolts.Bolt;
import com.crionuke.xaplus.events.*;
import com.crionuke.xaplus.stubs.XAResourceStub;
import com.opentable.db.postgres.embedded.DatabaseConnectionPreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class XAPlusJournalServiceTest extends XAPlusServiceTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusJournalServiceTest.class);

    @Rule
    public PreparedDbRule dbRule = EmbeddedPostgresRules.preparedDatabase(new TlogPreparer());

    XAPlusJournalService xaPlusJournalService;

    BlockingQueue<XAPlusCommitTransactionDecisionLoggedEvent> commitTransactionDecisionLoggedEvents;
    BlockingQueue<XAPlusRollbackTransactionDecisionLoggedEvent> rollbackTransactionDecisionLoggedEvents;
    BlockingQueue<XAPlusCommitRecoveredXidDecisionLoggedEvent> commitRecoveredXidDecisionLoggedEvents;
    BlockingQueue<XAPlusRollbackRecoveredXidDecisionLoggedEvent> rollbackRecoveredXidDecisionLoggedEvents;
    BlockingQueue<XAPlusDanglingTransactionsFoundEvent> danglingTransactionsFoundEvents;

    ConsumerStub consumerStub;

    @Before
    public void beforeTest() throws SQLException {
        createXAPlusComponents(1);
        engine.setTLogDataSource(dbRule.getTestDatabase());

        xaPlusJournalService = new XAPlusJournalService(properties, threadPool, dispatcher, engine);
        xaPlusJournalService.postConstruct();

        commitTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackTransactionDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        commitRecoveredXidDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        rollbackRecoveredXidDecisionLoggedEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);
        danglingTransactionsFoundEvents = new LinkedBlockingQueue<>(QUEUE_SIZE);

        consumerStub = new ConsumerStub();
        consumerStub.postConstruct();
    }

    @After
    public void afterTest() {
        xaPlusJournalService.finish();
        consumerStub.finish();
    }

    @Test
    public void testLogCommitTransactionDecision() throws InterruptedException, SQLException {
        XAPlusTransaction transaction = createSuperiorTransaction();
        XAPlusXid bxid1 = createBranchXid(transaction);
        transaction.enlist(bxid1, "db1", new XAResourceStub());
        XAPlusXid bxid2 = createBranchXid(transaction);
        transaction.enlist(bxid2, "db2", new XAResourceStub());
        XAPlusXid bxid3 = createBranchXid(transaction);
        transaction.enlist(bxid3, "db3", new XAResourceStub());
        dispatcher.dispatch(new XAPlusLogCommitTransactionDecisionEvent(transaction.getXid(),
                transaction.getUniqueNames()));
        XAPlusCommitTransactionDecisionLoggedEvent event = commitTransactionDecisionLoggedEvents
                .poll(POLL_TIMIOUT_MS, TimeUnit.MILLISECONDS);
    }

    @Test
    public void test2() {
        assertTrue(true);
    }

    private class ConsumerStub extends Bolt implements
            XAPlusCommitTransactionDecisionLoggedEvent.Handler,
            XAPlusRollbackTransactionDecisionLoggedEvent.Handler,
            XAPlusCommitRecoveredXidDecisionLoggedEvent.Handler,
            XAPlusRollbackRecoveredXidDecisionLoggedEvent.Handler,
            XAPlusDanglingTransactionsFoundEvent.Handler {

        ConsumerStub() {
            super("consumer-stub", QUEUE_SIZE);
        }

        @Override
        public void handleCommitTransactionDecisionLogged(XAPlusCommitTransactionDecisionLoggedEvent event)
                throws InterruptedException {
            commitTransactionDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleRollbackTransactionDecisionLogged(XAPlusRollbackTransactionDecisionLoggedEvent event)
                throws InterruptedException {
            rollbackTransactionDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleCommitRecoveredXidDecisionLogged(XAPlusCommitRecoveredXidDecisionLoggedEvent event)
                throws InterruptedException {
            commitRecoveredXidDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleRollbackRecoveredXidDecisionLogged(XAPlusRollbackRecoveredXidDecisionLoggedEvent event)
                throws InterruptedException {
            rollbackRecoveredXidDecisionLoggedEvents.put(event);
        }

        @Override
        public void handleDanglingTransactionFound(XAPlusDanglingTransactionsFoundEvent event) throws InterruptedException {
            danglingTransactionsFoundEvents.put(event);
        }

        void postConstruct() {
            threadPool.execute(this);
            dispatcher.subscribe(this, XAPlusCommitTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackTransactionDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusCommitRecoveredXidDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusRollbackRecoveredXidDecisionLoggedEvent.class);
            dispatcher.subscribe(this, XAPlusDanglingTransactionsFoundEvent.class);
        }
    }

    private class TlogPreparer implements DatabaseConnectionPreparer {

        @Override
        public void prepare(Connection conn) throws SQLException {
            String sql = "CREATE TABLE tlog (t_id bigserial PRIMARY KEY, " +
                    "t_timestamp timestamp NOT NULL, " +
                    "t_server_id varchar(64) NOT NULL, " +
                    "t_gtrid bytea, " +
                    "t_bqual bytea, " +
                    "t_unique_name varchar(64) NOT NULL, " +
                    "t_status varchar(1) NOT NULL);";
            try (PreparedStatement statement = conn.prepareStatement(sql)) {
                statement.execute();
            }
        }
    }
}
