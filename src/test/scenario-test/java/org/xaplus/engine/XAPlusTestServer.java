package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xaplus.engine.events.xaplus.*;

import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

class XAPlusTestServer implements XAPlusFactory, XAPlusResource {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusTestServer.class);

    private final XAPlusDispatcher dispatcher;

    XAPlusTestServer(XAPlusDispatcher dispatcher) {
        this.dispatcher = dispatcher;
    }

    @Override public XAPlusResource createXAPlusResource() throws XAPlusException {
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
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Got prepare request for xid={} from superior server", xaPlusXid);
            }
            dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToPrepareEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAException(e.getMessage());
        }
        return XA_OK;
    }

    @Override
    public void ready(Xid xid) throws XAPlusException {
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Got ready status for xid={} from subordinate server", xid);
            }
            dispatcher.dispatch(new XAPlusRemoteSubordinateReadyEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAPlusException(e.getMessage());
        }
    }

    @Override
    public void commit(Xid xid, boolean onePhase) throws XAException {
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Got commit request xid={} from superior server", xid);
            }
            dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToCommitEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAException(e.getMessage());
        }
    }

    @Override
    public void rollback(Xid xid) throws XAException {
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Got rollback request for xid={} from superior server", xid);
            }
            dispatcher.dispatch(new XAPlusRemoteSuperiorOrderToRollbackEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAException(e.getMessage());
        }
    }

    @Override
    public void done(Xid xid) throws XAPlusException {
        XAPlusXid xaPlusXid = new XAPlusXid(xid);
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Got done status for xid={} from subordinate server", xid);
            }
            dispatcher.dispatch(new XAPlusRemoteSubordinateDoneEvent(xaPlusXid));
        } catch (InterruptedException e) {
            throw new XAPlusException(e.getMessage());
        }
    }

    @Override
    public void retry(String serverId) throws XAPlusException {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug("Got retry request from subordinate server={}", serverId);
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
