package org.xaplus.engine.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryRequestEvent extends Event<XAPlusRecoveryRequestEvent.Handler> {

    public XAPlusRecoveryRequestEvent() {
        super();
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryServerRequest(this);
    }

    public interface Handler {
        void handleRecoveryServerRequest(XAPlusRecoveryRequestEvent event) throws InterruptedException;
    }
}