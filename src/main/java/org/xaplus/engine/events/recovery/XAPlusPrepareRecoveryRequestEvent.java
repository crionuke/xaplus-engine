package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusPrepareRecoveryRequestEvent extends Event<XAPlusPrepareRecoveryRequestEvent.Handler> {

    private final long inFlightCutoff;

    public XAPlusPrepareRecoveryRequestEvent(long inFlightCutoff) {
        super();
        this.inFlightCutoff = inFlightCutoff;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handlePrepareRecoveryRequest(this);
    }

    public long getInFlightCutoff() {
        return inFlightCutoff;
    }

    public interface Handler {
        void handlePrepareRecoveryRequest(XAPlusPrepareRecoveryRequestEvent event) throws InterruptedException;
    }
}