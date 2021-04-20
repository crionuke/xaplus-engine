package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusUserRecoveryRequestEvent extends Event<XAPlusUserRecoveryRequestEvent.Handler> {

    public XAPlusUserRecoveryRequestEvent() {
        super();
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleUserRecoveryRequest(this);
    }

    public interface Handler {
        void handleUserRecoveryRequest(XAPlusUserRecoveryRequestEvent event) throws InterruptedException;
    }
}