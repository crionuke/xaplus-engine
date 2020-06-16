package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusForgetRecoveredXidRequestEvent extends Event<XAPlusForgetRecoveredXidRequestEvent.Handler> {

    private final XAPlusXid xid;
    private final XAResource resource;

    public XAPlusForgetRecoveredXidRequestEvent(XAPlusXid xid, XAResource resource) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        this.xid = xid;
        this.resource = resource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleForgetRecoveredXidRequest(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleForgetRecoveredXidRequest(XAPlusForgetRecoveredXidRequestEvent event) throws InterruptedException;
    }
}