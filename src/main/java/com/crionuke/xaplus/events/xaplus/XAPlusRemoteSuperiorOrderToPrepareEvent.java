package com.crionuke.xaplus.events.xaplus;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRemoteSuperiorOrderToPrepareEvent extends Event<XAPlusRemoteSuperiorOrderToPrepareEvent.Handler> {

    private final XAPlusXid xid;

    public XAPlusRemoteSuperiorOrderToPrepareEvent(XAPlusXid xid) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xid = xid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRemoteSuperiorOrderToPrepare(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public interface Handler {
        void handleRemoteSuperiorOrderToPrepare(XAPlusRemoteSuperiorOrderToPrepareEvent event) throws InterruptedException;
    }
}
