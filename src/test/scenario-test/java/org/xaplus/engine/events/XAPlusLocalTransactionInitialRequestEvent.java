package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLocalTransactionInitialRequestEvent extends Event<XAPlusLocalTransactionInitialRequestEvent.Handler> {

    private final long value;

    public XAPlusLocalTransactionInitialRequestEvent(long value) {
        super();
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleLocalTransactionInitialRequest(this);
    }

    public long getValue() {
        return value;
    }

    public interface Handler {
        void handleLocalTransactionInitialRequest(XAPlusLocalTransactionInitialRequestEvent event)
                throws InterruptedException;
    }
}