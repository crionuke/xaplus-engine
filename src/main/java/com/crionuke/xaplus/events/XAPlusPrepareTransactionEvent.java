package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusPrepareTransactionEvent extends Event<XAPlusPrepareTransactionEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlusPrepareTransactionEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handlePrepareTransaction(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(transaction=" + transaction + ")";
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public interface Handler {
        void handlePrepareTransaction(XAPlusPrepareTransactionEvent event) throws InterruptedException;
    }
}