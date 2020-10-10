package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusCommitTransactionDecisionLoggedEvent extends Event<XAPlusCommitTransactionDecisionLoggedEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlusCommitTransactionDecisionLoggedEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleCommitTransactionDecisionLogged(this);
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public interface Handler {
        void handleCommitTransactionDecisionLogged(XAPlusCommitTransactionDecisionLoggedEvent event) throws InterruptedException;
    }
}