package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackTransactionDecisionFailedEvent extends Event<XAPlusRollbackTransactionDecisionFailedEvent.Handler> {

    private final XAPlusXid xid;
    private final Exception exception;

    public XAPlusRollbackTransactionDecisionFailedEvent(XAPlusXid xid, Exception exception) {
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

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackTransactionDecisionFailed(this);
    }

    public XAPlusXid getXid() {
        return xid;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleRollbackTransactionDecisionFailed(XAPlusRollbackTransactionDecisionFailedEvent event) throws InterruptedException;
    }
}