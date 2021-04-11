package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusStartRecoveryRequestEvent extends Event<XAPlusStartRecoveryRequestEvent.Handler> {

    public XAPlusStartRecoveryRequestEvent() {
        super();
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleStartRecoveryRequest(this);
    }

    public interface Handler {
        void handleStartRecoveryRequest(XAPlusStartRecoveryRequestEvent event) throws InterruptedException;
    }
}