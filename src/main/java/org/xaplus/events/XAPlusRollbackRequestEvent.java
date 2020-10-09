package org.xaplus.events;

import com.crionuke.bolts.Event;
import org.xaplus.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackRequestEvent extends Event<XAPlusRollbackRequestEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlusRollbackRequestEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackRequest(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(transaction=" + transaction + ")";
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public interface Handler {
        void handleRollbackRequest(XAPlusRollbackRequestEvent event) throws InterruptedException;
    }
}