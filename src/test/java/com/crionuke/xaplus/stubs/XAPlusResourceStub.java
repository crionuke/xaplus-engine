package com.crionuke.xaplus.stubs;

import com.crionuke.xaplus.XAPlusException;
import com.crionuke.xaplus.XAPlusResource;

import javax.transaction.xa.Xid;

public class XAPlusResourceStub extends XAResourceStub implements XAPlusResource {

    @Override
    public void ready(Xid xid) throws XAPlusException {

    }

    @Override
    public void done(Xid xid) throws XAPlusException {

    }

    @Override
    public void retry(String serverId) throws XAPlusException {

    }
}
