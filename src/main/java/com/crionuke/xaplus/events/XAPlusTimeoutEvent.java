package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTimeoutEvent extends Event<XAPlusTimeoutEvent.Handler> {

    private final XAPlusXid xid;

    public XAPlusTimeoutEvent(XAPlusXid xid) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        this.xid = xid;
    }

    public XAPlusXid getXid() {
        return xid;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTimeout(this);
    }

    public interface Handler {
        void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException;
    }
}