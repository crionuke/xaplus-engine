package org.xaplus.engine.events.journal;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusCompletedTransactionLoggedEvent extends Event<XAPlusCompletedTransactionLoggedEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlusCompletedTransactionLoggedEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleCompletedTransactionLogged(this);
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(transaction=" + transaction + ")";
    }

    public interface Handler {
        void handleCompletedTransactionLogged(XAPlusCompletedTransactionLoggedEvent event) throws InterruptedException;
    }
}