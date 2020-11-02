package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusScenarioSuperiorFinishedEvent extends Event<XAPlusScenarioSuperiorFinishedEvent.Handler> {

    private final boolean status;
    private final long value;

    public XAPlusScenarioSuperiorFinishedEvent(boolean status, long value) {
        super();
        this.status = status;
        this.value = value;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleScenarioSuperiorFinished(this);
    }

    public boolean getStatus() {
        return status;
    }

    public long getValue() {
        return value;
    }

    public interface Handler {
        void handleScenarioSuperiorFinished(XAPlusScenarioSuperiorFinishedEvent event) throws InterruptedException;
    }
}