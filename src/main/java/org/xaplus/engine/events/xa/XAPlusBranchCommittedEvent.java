package org.xaplus.engine.events.xa;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusBranchCommittedEvent extends Event<XAPlusBranchCommittedEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusXid branchXid;

    public XAPlusBranchCommittedEvent(XAPlusXid xid, XAPlusXid branchXid) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (branchXid == null) {
            throw new NullPointerException("branchXid is null");
        }
        this.xid = xid;
        this.branchXid = branchXid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleBranchCommitted(this);
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

    public interface Handler {
        void handleBranchCommitted(XAPlusBranchCommittedEvent event) throws InterruptedException;
    }
}