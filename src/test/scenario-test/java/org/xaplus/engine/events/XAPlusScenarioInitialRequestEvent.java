package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusScenarioInitialRequestEvent extends Event<XAPlusScenarioInitialRequestEvent.Handler> {

    private final int value;

    public XAPlusScenarioInitialRequestEvent(int value) {
        super();
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleScenarioInitialRequest(this);
    }

    public int getValue() {
        return value;
    }

    public interface Handler {
        void handleScenarioInitialRequest(XAPlusScenarioInitialRequestEvent event) throws InterruptedException;
    }
}