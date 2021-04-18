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

    protected DataSource createTLogDataSource() {
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

    class XAPlusTestTransaction {

        private final XAConnection xaConnection;
        private final XAResource xaResource;
        private final Connection connection;
        private final XAPlusXid xid;
        private final XAPlusXid branchXid;

        XAPlusTestTransaction(XADataSource xaDataSource, String serverId) throws SQLException {
            this.xaConnection = xaDataSource.getXAConnection();
            this.xaResource = xaConnection.getXAResource();
            this.connection = xaConnection.getConnection();
            this.xid = new XAPlusXid(new XAPlusUid(serverId), new XAPlusUid(serverId));
            this.branchXid = new XAPlusXid(xid.getGtrid(), serverId);
        }

        XAResource getXaResource() {
            return xaResource;
        }

        XAPlusXid getXid() {
            return xid;
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

        void close() throws SQLException {
            connection.close();
            xaConnection.close();
        }
    }
}
