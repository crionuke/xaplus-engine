package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.xaplus.*;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

class XAPlusTestServer implements XAPlusFactory, XAPlusResource {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTestServer.class);

    private final XAPlusScenarioExceptions scenario;
    private final XAPlusDispatcher dispatcher;

    XAPlusTestServer(XAPlusScenarioExceptions scenario, XAPlusDispatcher dispatcher) {
        this.scenario = scenario;
        this.dispatcher = dispatcher;
    }

    @Override
    public XAPlusResource createXAPlusResource() throws XAPlusException {
        return this;
    }

    @Override
    public void start(Xid xid, int flags) throws XAException {
    }

    @Override
    public void end(Xid xid, int flags) throws XAException {
    }

    @Override
    public int prepare(Xid xid) throws XAException {
        return XA_OK;
    }

    @Override
    public void readied(Xid xid) throws XAPlusException {
        if (scenario.readiedException) {
            throw new XAPlusException("readied request exception");
        }
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Send readied status from subordinate server, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusRemoteSubordinateReadiedEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAPlusException(e.getMessage());
        }
    }

    @Override
    public void failed(Xid xid) throws XAPlusException {
        if (scenario.failedException) {
            throw new XAPlusException("failed request exception");
        }
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Send failed status from subordinate server, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusRemoteSubordinateFailedEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAPlusException(e.getMessage());
        }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        if (scenario.commitException) {
            throw new XAException("commit request exception");
        }
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Send commit request from superior server, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToCommitEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAException(e.getMessage());
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        if (scenario.rollbackException) {
            throw new XAException("rollback request exception");
        }
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Send rollback request from superior server, xid={}", xid);
            }
            dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAException(e.getMessage());
        }
    }

    @Override
    public void retry(String serverId) throws XAPlusException {
        if (scenario.retryException) {
            throw new XAPlusException("retry request exception");
        }
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Send retry request from subordinate server, serverId={}", serverId);
            }
            dispatcher.dispatch(new XAPlusRemoteSubordinateRetryRequestEvent(serverId));
        } catch (InterruptedException e) {
            throw new XAPlusException(e.getMessage());
        }
    }

    @Override
    public Xid[] recover(int flag) throws XAException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void forget(Xid xid) throws XAException {
        throw new UnsupportedOperationException();
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
    public boolean setTransactionTimeout(int seconds) throws XAException {
        return false;
    }
}
