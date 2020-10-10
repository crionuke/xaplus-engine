package org.xaplus.engine.events.twopc;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlus2pcDoneEvent extends Event<XAPlus2pcDoneEvent.Handler> {

    private final XAPlusTransaction transaction;

    public XAPlus2pcDoneEvent(XAPlusTransaction transaction) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handle2pcDone(this);
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public interface Handler {
        void handle2pcDone(XAPlus2pcDoneEvent event) throws InterruptedException;
    }
}