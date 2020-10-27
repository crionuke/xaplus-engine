package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusScenarioFinishedEvent extends Event<XAPlusScenarioFinishedEvent.Handler> {

    private final long value;

    public XAPlusScenarioFinishedEvent(long value) {
        super();
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleScenarioFinished(this);
    }

    public long getValue() {
        return value;
    }

    public interface Handler {
        void handleScenarioFinished(XAPlusScenarioFinishedEvent event) throws InterruptedException;
    }
}