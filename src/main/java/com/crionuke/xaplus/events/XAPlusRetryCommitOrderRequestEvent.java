package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusResource;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRetryCommitOrderRequestEvent extends Event<XAPlusRetryCommitOrderRequestEvent.Handler> {

    private final XAPlusXid xid;
    private final XAPlusResource resource;

    public XAPlusRetryCommitOrderRequestEvent(XAPlusXid xid, XAPlusResource resource) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (resource == null) {
            throw new NullPointerException("resource is null");
        }
        this.xid = xid;
        this.resource = resource;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRetryCommitOrderRequest(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public XAPlusResource getResource() {
        return resource;
    }

    public interface Handler {
        void handleRetryCommitOrderRequest(XAPlusRetryCommitOrderRequestEvent event) throws InterruptedException;
    }
}