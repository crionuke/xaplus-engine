package org.xaplus.engine.stubs;

import org.xaplus.engine.XAPlusException;
import org.xaplus.engine.XAPlusResource;

import javax.transaction.xa.Xid;

public class XAPlusResourceStub extends XAResourceStub implements XAPlusResource {

    @Override
    public void readied(Xid xid) throws XAPlusException {

    }

    @Override
    public void failed(Xid xid) throws XAPlusException {

    }

    @Override
    public void done(Xid xid) throws XAPlusException {

    }

    @Override
    public void retry(String serverId) throws XAPlusException {

    }
}
