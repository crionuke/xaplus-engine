package org.xaplus.engine.events;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTimeoutEvent extends Event<XAPlusTimeoutEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlusTimeoutEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTimeout(this);
    }

    public interface Handler {
        void handleTimeout(XAPlusTimeoutEvent event) throws InterruptedException;
    }
}