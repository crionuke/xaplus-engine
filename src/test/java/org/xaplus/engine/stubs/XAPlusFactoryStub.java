package org.xaplus.engine.stubs;

import org.xaplus.engine.XAPlusException;
import org.xaplus.engine.XAPlusFactory;
import org.xaplus.engine.XAPlusResource;

public class XAPlusFactoryStub implements XAPlusFactory {

    @Override
    public XAPlusResource createXAPlusResource() throws XAPlusException {
        return new XAPlusResourceStub();
    }
}
