package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusLocalScenarioInitialRequestEvent extends Event<XAPlusLocalScenarioInitialRequestEvent.Handler> {

    private final long value;

    public XAPlusLocalScenarioInitialRequestEvent(long value) {
        super();
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleLocalScenarioInitialRequest(this);
    }

    public long getValue() {
        return value;
    }

    public interface Handler {
        void handleLocalScenarioInitialRequest(XAPlusLocalScenarioInitialRequestEvent event)
                throws InterruptedException;
    }
}