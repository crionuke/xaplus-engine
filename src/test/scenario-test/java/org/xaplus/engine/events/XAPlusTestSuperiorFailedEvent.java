package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTestSuperiorFailedEvent extends Event<XAPlusTestSuperiorFailedEvent.Handler> {

    private final long value;
    private final Exception exception;

    public XAPlusTestSuperiorFailedEvent(long value, Exception exception) {
        super();
        this.value = value;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTestSuperiorFailed(this);
    }

    public long getValue() {
        return value;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleTestSuperiorFailed(XAPlusTestSuperiorFailedEvent event) throws InterruptedException;
    }
}