package org.xaplus.engine.events.journal;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLogCompletedTransactionEvent extends Event<XAPlusLogCompletedTransactionEvent.Handler> {

    private final XAPlusTransaction transaction;
    private final boolean status;

    public XAPlusLogCompletedTransactionEvent(XAPlusTransaction transaction, boolean status) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
        this.status = status;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleLogTransactionCompleted(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(transaction=" + transaction + ", status=" + status + ")";
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public boolean getStatus() {
        return status;
    }

    public interface Handler {
        void handleLogTransactionCompleted(XAPlusLogCompletedTransactionEvent event) throws InterruptedException;
    }
}