package org.xaplus.engine.events.xa;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusCommitBranchRequestEvent extends Event<XAPlusCommitBranchRequestEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusXid branchXid;
    private final XAResource resource;

    public XAPlusCommitBranchRequestEvent(XAPlusXid xid, XAPlusXid branchXid, XAResource resource) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (branchXid == null) {
            throw new NullPointerException("branchXid is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        this.xid = xid;
        this.branchXid = branchXid;
        this.resource = resource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleCommitBranchRequest(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(xid=" + xid + ", branchXid=" + branchXid + ")";
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusXid getBranchXid() {
        return branchXid;
    }

    public XAResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleCommitBranchRequest(XAPlusCommitBranchRequestEvent event) throws InterruptedException;
    }
}