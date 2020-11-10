package org.xaplus.engine.events.journal;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLogComplettedTransactionFailedEvent extends Event<XAPlusLogComplettedTransactionFailedEvent.Handler> {

    private final XAPlusTransaction transaction;
    private final boolean status;
    private final Exception exception;

    public XAPlusLogComplettedTransactionFailedEvent(XAPlusTransaction transaction, boolean status,
                                                     Exception exception) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        if (exception == null) {
            throw new NullPointerException("exception is null");
        }
        this.transaction = transaction;
        this.status = status;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleLogCompletedTransactionFailed(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(transaction=" + transaction + ", exception=" + exception + ")";
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public Exception getException() {
        return exception;
    }

    public boolean getStatus() {
        return status;
    }

    public interface Handler {
        void handleLogCompletedTransactionFailed(XAPlusLogComplettedTransactionFailedEvent event) throws InterruptedException;
    }
}