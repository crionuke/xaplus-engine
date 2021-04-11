package org.xaplus.engine.stubs;

import org.xaplus.engine.XAPlusException;
import org.xaplus.engine.XAPlusFactory;
import org.xaplus.engine.XAPlusResource;

public class XAPlusFactoryStub implements XAPlusFactory {

    private final XAPlusResource xaPlusResource;

    public XAPlusFactoryStub() {
        xaPlusResource = new XAPlusResourceStub();
    }

    @Override
    public XAPlusResource createXAPlusResource() throws XAPlusException {
        return xaPlusResource;
    }
}
