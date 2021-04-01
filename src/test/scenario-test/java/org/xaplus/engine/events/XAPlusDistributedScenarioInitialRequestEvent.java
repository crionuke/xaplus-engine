package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusDistributedScenarioInitialRequestEvent extends Event<XAPlusDistributedScenarioInitialRequestEvent.Handler> {

    private final long value;

    public XAPlusDistributedScenarioInitialRequestEvent(long value) {
        super();
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleDistributedScenarioInitialRequest(this);
    }

    public long getValue() {
        return value;
    }

    public interface Handler {
        void handleDistributedScenarioInitialRequest(XAPlusDistributedScenarioInitialRequestEvent event)
                throws InterruptedException;
    }
}