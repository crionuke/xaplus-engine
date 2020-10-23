package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusScenarioFailedEvent extends Event<XAPlusScenarioFailedEvent.Handler> {

    private final int value;
    private final Exception exception;

    public XAPlusScenarioFailedEvent(int value, Exception exception) {
        super();
        this.value = value;
        this.exception = exception;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleScenarioFinished(this);
    }

    public int getValue() {
        return value;
    }

    public Exception getException() {
        return exception;
    }

    public interface Handler {
        void handleScenarioFinished(XAPlusScenarioFailedEvent event) throws InterruptedException;
    }
}