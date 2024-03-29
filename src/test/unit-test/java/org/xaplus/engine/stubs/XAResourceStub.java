package org.xaplus.engine.stubs;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

public class XAResourceStub implements XAResource {

    private final Xid[] recoverResponse;

    public XAResourceStub() {
        this.recoverResponse = new Xid[0];
    }

    public XAResourceStub(Xid[] recoverResponse) {
        this.recoverResponse = recoverResponse;
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {

    }

    @Override
    public void end(Xid xid, int flags) throws XAException {

    }

    @Override
    public void forget(Xid xid) throws XAException {

    }

    @Override
    public int getTransactionTimeout() throws XAException {
        return 0;
    }

    @Override
    public boolean isSameRM(XAResource xares) throws XAException {
        return false;
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return 0;
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        return recoverResponse;
    }

    @Override
    public void rollback(Xid xid) throws XAException {

    }

    @Override
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return false;
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {

    }
}
