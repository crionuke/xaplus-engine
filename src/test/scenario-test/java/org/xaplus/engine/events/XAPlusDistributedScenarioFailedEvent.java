package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusDistributedScenarioFailedEvent extends Event<XAPlusDistributedScenarioFailedEvent.Handler> {

    private final long value;
    private final Exception exception;

    public XAPlusDistributedScenarioFailedEvent(long value, Exception exception) {
        super();
        this.value = value;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleDistributedScenarioFailed(this);
    }

    public long getValue() {
        return value;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleDistributedScenarioFailed(XAPlusDistributedScenarioFailedEvent event) throws InterruptedException;
    }
}