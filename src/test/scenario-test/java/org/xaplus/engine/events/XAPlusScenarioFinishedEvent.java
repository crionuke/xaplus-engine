package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusScenarioFinishedEvent extends Event<XAPlusScenarioFinishedEvent.Handler> {

    private final int value;

    public XAPlusScenarioFinishedEvent(int value) {
        super();
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleScenarioFinished(this);
    }

    public int getValue() {
        return value;
    }

    public interface Handler {
        void handleScenarioFinished(XAPlusScenarioFinishedEvent event) throws InterruptedException;
    }
}