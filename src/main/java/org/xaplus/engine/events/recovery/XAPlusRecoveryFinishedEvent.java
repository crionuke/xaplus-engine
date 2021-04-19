package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryFinishedEvent extends Event<XAPlusRecoveryFinishedEvent.Handler> {

    private final Set<XAPlusXid> finishedXids;

    public XAPlusRecoveryFinishedEvent(Set<XAPlusXid> finishedXids) {
        super();
        this.finishedXids = ConcurrentHashMap.newKeySet();
        this.finishedXids.addAll(finishedXids);
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryFinished(this);
    }

    public Set<XAPlusXid> getFinishedXids() {
        return finishedXids;
    }

    public interface Handler {
        void handleRecoveryFinished(XAPlusRecoveryFinishedEvent event) throws InterruptedException;
    }
}