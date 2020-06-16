package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusBranchPreparedEvent extends Event<XAPlusBranchPreparedEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusXid branchXid;

    public XAPlusBranchPreparedEvent(XAPlusXid xid, XAPlusXid branchXid) {
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
        handler.handleBranchPrepared(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusXid getBranchXid() {
        return branchXid;
    }

    public interface Handler {
        void handleBranchPrepared(XAPlusBranchPreparedEvent event) throws InterruptedException;
    }
}