package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusTransaction;
import com.crionuke.xaplus.XAPlusXid;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRollbackTransactionDecisionLoggedEvent extends Event<XAPlusRollbackTransactionDecisionLoggedEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlusRollbackTransactionDecisionLoggedEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRollbackTransactionDecisionLogged(this);
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public interface Handler {
        void handleRollbackTransactionDecisionLogged(XAPlusRollbackTransactionDecisionLoggedEvent event) throws InterruptedException;
    }
}