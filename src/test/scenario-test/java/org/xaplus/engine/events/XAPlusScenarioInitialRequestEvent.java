package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusScenarioInitialRequestEvent extends Event<XAPlusScenarioInitialRequestEvent.Handler> {

    private final int value;
    private final boolean userRollback;

    public XAPlusScenarioInitialRequestEvent(int value, boolean userRollback) {
        super();
        this.value = value;
        this.userRollback = userRollback;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleScenarioInitialRequest(this);
    }

    public int getValue() {
        return value;
    }

    public boolean isUserRollback() {
        return userRollback;
    }

    public interface Handler {
        void handleScenarioInitialRequest(XAPlusScenarioInitialRequestEvent event) throws InterruptedException;
    }
}