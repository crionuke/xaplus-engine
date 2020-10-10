package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

import javax.transaction.xa.XAResource;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRetryRollbackBranchRequestEvent extends Event<XAPlusRetryRollbackBranchRequestEvent.Handler> {

    private final XAPlusXid branchXid;
    private final XAResource resource;

    public XAPlusRetryRollbackBranchRequestEvent(XAPlusXid branchXid, XAResource resource) {
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
        handler.handleRetryRollbackBranchRequest(this);
    }

    public XAPlusXid getBranchXid() {
        return branchXid;
    }

    public XAResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleRetryRollbackBranchRequest(XAPlusRetryRollbackBranchRequestEvent event) throws InterruptedException;
    }
}