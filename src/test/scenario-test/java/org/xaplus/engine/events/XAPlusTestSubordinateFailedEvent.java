package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTestSubordinateFailedEvent extends Event<XAPlusTestSubordinateFailedEvent.Handler> {

    private final long value;
    private final Exception exception;

    public XAPlusTestSubordinateFailedEvent(long value, Exception exception) {
        super();
        this.value = value;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTestSubordinateFailed(this);
    }

    public long getValue() {
        return value;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleTestSubordinateFailed(XAPlusTestSubordinateFailedEvent event) throws InterruptedException;
    }
}