package org.xaplus.events;

import com.crionuke.bolts.Event;
import org.xaplus.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackFailedEvent extends Event<XAPlusRollbackFailedEvent.Handler> {

    private final XAPlusTransaction transaction;
    private final Exception exception;

    public XAPlusRollbackFailedEvent(XAPlusTransaction transaction, Exception exception) {
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

    public XAPlusTransaction getTransaction() {
        return transaction;
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