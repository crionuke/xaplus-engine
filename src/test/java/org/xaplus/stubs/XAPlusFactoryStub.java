package org.xaplus.stubs;

import org.xaplus.XAPlusException;
import org.xaplus.XAPlusFactory;
import org.xaplus.XAPlusResource;

public class XAPlusFactoryStub implements XAPlusFactory {

    @Override
    public XAPlusResource createXAPlusResource() throws XAPlusException {
        return new XAPlusResourceStub();
    }
}
