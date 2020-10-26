package org.xaplus.engine.events.rollback;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTransactionRolledBackEvent extends Event<XAPlusTransactionRolledBackEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlusTransactionRolledBackEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTransactionRolledBack(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(transaction=" + transaction + ")";
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public interface Handler {
        void handleTransactionRolledBack(XAPlusTransactionRolledBackEvent event) throws InterruptedException;
    }
}