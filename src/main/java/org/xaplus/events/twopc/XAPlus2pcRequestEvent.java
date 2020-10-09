package org.xaplus.events.twopc;

import com.crionuke.bolts.Event;
import org.xaplus.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlus2pcRequestEvent extends Event<XAPlus2pcRequestEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlus2pcRequestEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handle2pcRequest(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(transaction=" + transaction + ")";
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public interface Handler {
        void handle2pcRequest(XAPlus2pcRequestEvent event) throws InterruptedException;
    }
}