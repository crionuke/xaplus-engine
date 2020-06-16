package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusBranchReadOnlyEvent extends Event<XAPlusBranchReadOnlyEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusXid branchXid;

    public XAPlusBranchReadOnlyEvent(XAPlusXid xid, XAPlusXid branchXid) {
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
        handler.handleBranchReadOnly(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusXid getBranchXid() {
        return branchXid;
    }

    public interface Handler {
        void handleBranchReadOnly(XAPlusBranchReadOnlyEvent event) throws InterruptedException;
    }
}