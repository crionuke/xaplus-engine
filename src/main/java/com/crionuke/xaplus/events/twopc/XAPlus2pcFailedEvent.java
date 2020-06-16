package com.crionuke.xaplus.events.twopc;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlus2pcFailedEvent extends Event<XAPlus2pcFailedEvent.Handler> {

    private final XAPlusTransaction transaction;
    private final Exception exception;

    public XAPlus2pcFailedEvent(XAPlusTransaction transaction, Exception exception) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        if (exception == null) {
            throw new NullPointerException("v is null");
        }
        this.transaction = transaction;
        this.exception = exception;
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public Exception getException() {
        return exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handle2pcFailed(this);
    }

    public interface Handler {
        void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException;
    }
}