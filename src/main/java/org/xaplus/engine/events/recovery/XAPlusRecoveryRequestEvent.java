package org.xaplus.engine.events.recovery;

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
        handler.handleRecoveryRequest(this);
    }

    public interface Handler {
        void handleRecoveryRequest(XAPlusRecoveryRequestEvent event) throws InterruptedException;
    }
}