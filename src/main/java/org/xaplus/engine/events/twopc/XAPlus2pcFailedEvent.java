package org.xaplus.engine.events.twopc;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusTransaction;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlus2pcFailedEvent extends Event<XAPlus2pcFailedEvent.Handler> {

    private final XAPlusTransaction transaction;
    private final boolean rollback;

    public XAPlus2pcFailedEvent(XAPlusTransaction transaction, boolean rollback) {
        super();
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
        this.rollback = rollback;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handle2pcFailed(this);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "=(transaction=" + transaction + ", v=" + rollback + ")";
    }

    public XAPlusTransaction getTransaction() {
        return transaction;
    }

    public boolean needRollback() {
        return rollback;
    }

    public interface Handler {
        void handle2pcFailed(XAPlus2pcFailedEvent event) throws InterruptedException;
    }
}