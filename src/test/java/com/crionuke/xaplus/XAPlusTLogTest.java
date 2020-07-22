package com.crionuke.xaplus;

import com.opentable.db.postgres.embedded.DatabaseConnectionPreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class XAPlusTLogTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTLogTest.class);

    @Rule
    public PreparedDbRule dbRule = EmbeddedPostgresRules.preparedDatabase(new TlogPreparer());

    // TODO

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
