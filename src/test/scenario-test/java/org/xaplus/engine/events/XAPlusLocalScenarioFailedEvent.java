package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLocalScenarioFailedEvent extends Event<XAPlusLocalScenarioFailedEvent.Handler> {

    private final long value;
    private final Exception exception;

    public XAPlusLocalScenarioFailedEvent(long value, Exception exception) {
        super();
        this.value = value;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleLocalScenarioFailed(this);
    }

    public long getValue() {
        return value;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleLocalScenarioFailed(XAPlusLocalScenarioFailedEvent event) throws InterruptedException;
    }
}