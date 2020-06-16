package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackFailedEvent extends Event<XAPlusRollbackFailedEvent.Handler> {

    private final XAPlusXid xid;
    private final Exception exception;

    public XAPlusRollbackFailedEvent(XAPlusXid xid, Exception exception) {
        super();
        if (xid == null) {
            throw new NullPointerException("xid is null");
        }
        if (exception == null) {
            throw new NullPointerException("exception is null");
        }
        this.xid = xid;
        this.exception = exception;
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackFailed(this);
    }

    public interface Handler {
        void handleRollbackFailed(XAPlusRollbackFailedEvent event) throws InterruptedException;
    }
}