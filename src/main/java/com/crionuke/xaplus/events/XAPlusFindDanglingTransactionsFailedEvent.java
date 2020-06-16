package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusFindDanglingTransactionsFailedEvent extends Event<XAPlusFindDanglingTransactionsFailedEvent.Handler> {

    private final Exception exception;

    public XAPlusFindDanglingTransactionsFailedEvent(Exception exception) {
        super();
        if (exception == null) {
            throw new NullPointerException("exception is null");
        }
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleFindDanglingTransactionsFailed(this);
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleFindDanglingTransactionsFailed(XAPlusFindDanglingTransactionsFailedEvent event) throws InterruptedException;
    }
}