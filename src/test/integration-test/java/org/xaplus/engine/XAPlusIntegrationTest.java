package org.xaplus.engine;

import org.apache.tomcat.jdbc.pool.DataSource;
import org.postgresql.xa.PGXADataSource;

import javax.sql.XAConnection;
import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class XAPlusIntegrationTest extends XAPlusUnitTest {

    static private final String INSERT_SQL = "INSERT INTO test (t_value) VALUES (?)";

    protected DataSource createTLog() {
        DataSource dataSource = new DataSource();
        dataSource.setUrl("jdbc:postgresql://localhost:10000/tlog");
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUsername("tlog");
        dataSource.setPassword("qwe123");
        return dataSource;
    }

    protected XADataSource createXADataSource() {
        PGXADataSource xaDataSource = new PGXADataSource();
        xaDataSource.setUrl("jdbc:postgresql://localhost:10001/test");
        xaDataSource.setUser("test");
        xaDataSource.setPassword("qwe123");
        return xaDataSource;
    }

    class TestResource implements AutoCloseable {

        protected final XAConnection xaConnection;
        protected final XAResource xaResource;
        protected final Connection connection;

        TestResource(XADataSource xaDataSource) throws SQLException {
            xaConnection = xaDataSource.getXAConnection();
            xaResource = xaConnection.getXAResource();
            connection = xaConnection.getConnection();
        }

        @Override
        public void close() throws SQLException {
            xaConnection.close();
        }
    }

    class OneBranchTransaction extends TestResource{

        private final XAPlusXid xid;

        OneBranchTransaction(XADataSource xaDataSource, String serverId) throws SQLException {
            super(xaDataSource);
            xid = new XAPlusXid(XAPlusUid.generate(serverId), XAPlusUid.generate(serverId));
        }

        XAPlusXid getXid() {
            return xid;
        }

        void start() throws XAException {
            xaResource.start(xid, XAResource.TMNOFLAGS);
        }

        void end() throws XAException {
            xaResource.end(xid, XAResource.TMSUCCESS);
        }

        void prepare() throws XAException {
            int vote = xaResource.prepare(xid);
            assertEquals(0, vote);
        }

        void insert() throws SQLException {
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_SQL);
            long value = Math.round(Math.random() * 1000);
            preparedStatement.setLong(1, value);
            int affected = preparedStatement.executeUpdate();
            assertEquals(1, affected);
            preparedStatement.close();
        }
    }
}
