package com.crionuke.xaplus.stubs;

import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.Connection;
import java.sql.SQLException;

public class XAConnectionStub implements XAConnection {

    @Override
    public XAResource getXAResource() throws SQLException {
        return new XAResourceStub();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new ConnectionStub();
    }

    @Override
    public void close() throws SQLException {

    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {

    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {

    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {

    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {

    }
}
