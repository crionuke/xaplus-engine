package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusTransaction;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackTransactionDecisionFailedEvent extends Event<XAPlusRollbackTransactionDecisionFailedEvent.Handler> {

    private final XAPlusTransaction transaction;
    private final Exception exception;

    public XAPlusRollbackTransactionDecisionFailedEvent(XAPlusTransaction transaction, Exception exception) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        if (exception == null) {
            throw new NullPointerException("exception is null");
        }
        this.transaction = transaction;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackTransactionDecisionFailed(this);
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleRollbackTransactionDecisionFailed(XAPlusRollbackTransactionDecisionFailedEvent event) throws InterruptedException;
    }
}