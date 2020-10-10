package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRetryCommitBranchRequestEvent extends Event<XAPlusRetryCommitBranchRequestEvent.Handler> {

    private final XAPlusXid branchXid;
    private final XAResource resource;

    public XAPlusRetryCommitBranchRequestEvent(XAPlusXid branchXid, XAResource resource) {
        super();
        if (branchXid == null) {
            throw new NullPointerException("branchXid is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        this.branchXid = branchXid;
        this.resource = resource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRetryCommitBranchRequest(this);
    }

    public XAPlusXid getBranchXid() {
        return branchXid;
    }

    public XAResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleRetryCommitBranchRequest(XAPlusRetryCommitBranchRequestEvent event) throws InterruptedException;
    }
}