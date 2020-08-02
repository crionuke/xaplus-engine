package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusTransaction;
import com.crionuke.xaplus.XAPlusXid;

import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLogCommitTransactionDecisionEvent extends Event<XAPlusLogCommitTransactionDecisionEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlusLogCommitTransactionDecisionEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleLogCommitTransactionDecision(this);
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public interface Handler {
        void handleLogCommitTransactionDecision(XAPlusLogCommitTransactionDecisionEvent event) throws InterruptedException;
    }
}