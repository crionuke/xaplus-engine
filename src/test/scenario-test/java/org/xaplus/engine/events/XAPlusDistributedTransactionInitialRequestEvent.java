package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusDistributedTransactionInitialRequestEvent extends Event<XAPlusDistributedTransactionInitialRequestEvent.Handler> {

    private final long value;

    public XAPlusDistributedTransactionInitialRequestEvent(long value) {
        super();
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleDistributedTransactionInitialRequest(this);
    }

    public long getValue() {
        return value;
    }

    public interface Handler {
        void handleDistributedTransactionInitialRequest(XAPlusDistributedTransactionInitialRequestEvent event)
                throws InterruptedException;
    }
}