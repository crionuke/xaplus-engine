package org.xaplus.engine;

import org.postgresql.xa.PGXADataSource;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class XAPlusIntegrationTest extends XAPlusUnitTest {

    static private final String INSERT_SQL = "INSERT INTO test (t_value) VALUES (?)";
    static private final String SELECT_SQL = "SELECT t_value FROM test";

    protected PGXADataSource xaDataSource;

    protected void createXADataSource() {
        xaDataSource = new PGXADataSource();
        xaDataSource.setUrl("jdbc:postgresql://localhost:10001/test");
        xaDataSource.setUser("test");
        xaDataSource.setPassword("qwe123");
    }

    class TestResource implements AutoCloseable {

        final XAConnection xaConnection;
        final XAResource xaResource;
        final Connection connection;

        TestResource() throws SQLException {
            xaConnection = xaDataSource.getXAConnection();
            xaResource = xaConnection.getXAResource();
            connection = xaConnection.getConnection();
        }

        XAResource getXaResource() {
            return xaResource;
        }

        @Override
        public void close() throws Exception {
            connection.close();
            xaConnection.close();
        }
    }

    class OneBranchTransaction extends TestResource {

        private final XAPlusTransaction xaPlusTransaction;
        private final XAPlusXid branchXid;

        OneBranchTransaction(String serverId) throws SQLException {
            super();
            XAPlusXid xid = new XAPlusXid(XAPlusUid.generate(serverId), XAPlusUid.generate(serverId));
            xaPlusTransaction = new XAPlusTransaction(xid, properties.getDefaultTimeoutInSeconds(), serverId);
            branchXid = XAPlusXid.generate(xid.getGlobalTransactionIdUid(), serverId);
        }

        XAPlusXid getXid() {
            return xaPlusTransaction.getXid();
        }

        XAPlusXid getBranchXid() {
            return branchXid;
        }

        void start() throws XAException {
            xaResource.start(branchXid, XAResource.TMNOFLAGS);
        }

        void end() throws XAException {
            xaResource.end(branchXid, XAResource.TMSUCCESS);
        }

        void prepare() throws XAException {
            int vote = xaResource.prepare(branchXid);
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
