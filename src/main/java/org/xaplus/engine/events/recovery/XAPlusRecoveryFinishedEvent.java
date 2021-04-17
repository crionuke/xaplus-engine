package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryFinishedEvent extends Event<XAPlusRecoveryFinishedEvent.Handler> {

    // TODO: fire finished xids after recovery

    public XAPlusRecoveryFinishedEvent() {
        super();
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryFinished(this);
    }

    public interface Handler {
        void handleRecoveryFinished(XAPlusRecoveryFinishedEvent event) throws InterruptedException;
    }
}