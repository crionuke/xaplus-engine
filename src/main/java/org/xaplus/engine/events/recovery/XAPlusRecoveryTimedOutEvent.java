package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;

import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryTimedOutEvent extends Event<XAPlusRecoveryTimedOutEvent.Handler> {

    public XAPlusRecoveryTimedOutEvent() {
        super();
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryTimedOut(this);
    }

    public interface Handler {
        void handleRecoveryTimedOut(XAPlusRecoveryTimedOutEvent event) throws InterruptedException;
    }
}