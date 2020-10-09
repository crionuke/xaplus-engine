package org.xaplus.events;

import com.crionuke.bolts.Event;
import org.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackBranchFailedEvent extends Event<XAPlusRollbackBranchFailedEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusXid branchXid;
    private final Exception exception;

    public XAPlusRollbackBranchFailedEvent(XAPlusXid xid, XAPlusXid branchXid, Exception exception) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (branchXid == null) {
            throw new NullPointerException("branchXid is null");
        }
        if (exception == null) {
            throw new NullPointerException("exception is null");
        }
        this.xid = xid;
        this.branchXid = branchXid;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackBranchFailed(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusXid getBranchXid() {
        return branchXid;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleRollbackBranchFailed(XAPlusRollbackBranchFailedEvent event) throws InterruptedException;
    }
}