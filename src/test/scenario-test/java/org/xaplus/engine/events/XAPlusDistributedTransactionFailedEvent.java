package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusDistributedTransactionFailedEvent extends Event<XAPlusDistributedTransactionFailedEvent.Handler> {

    private final long value;
    private final Exception exception;

    public XAPlusDistributedTransactionFailedEvent(long value, Exception exception) {
        super();
        this.value = value;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleDistributedTransactionFailed(this);
    }

    public long getValue() {
        return value;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleDistributedTransactionFailed(XAPlusDistributedTransactionFailedEvent event) throws InterruptedException;
    }
}