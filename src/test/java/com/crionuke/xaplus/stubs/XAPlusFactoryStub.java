package com.crionuke.xaplus.stubs;

import com.crionuke.xaplus.XAPlusException;
import com.crionuke.xaplus.XAPlusFactory;
import com.crionuke.xaplus.XAPlusResource;

public class XAPlusFactoryStub implements XAPlusFactory {

    @Override
    public XAPlusResource createXAPlusResource() throws XAPlusException {
        return new XAPlusResourceStub();
    }
}
