package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackRecoveredXidRequestEvent extends Event<XAPlusRollbackRecoveredXidRequestEvent.Handler> {

    private final XAPlusXid xid;
    private final XAResource resource;
    private final String uniqueName;

    public XAPlusRollbackRecoveredXidRequestEvent(XAPlusXid xid, XAResource resource, String uniqueName) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        if (uniqueName == null) {
            throw new NullPointerException("uniqueName is null");
        }
        this.xid = xid;
        this.resource = resource;
        this.uniqueName = uniqueName;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackRecoveredXidRequest(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAResource getResource() {
        return resource;
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public interface Handler {
        void handleRollbackRecoveredXidRequest(XAPlusRollbackRecoveredXidRequestEvent event) throws InterruptedException;
    }
}