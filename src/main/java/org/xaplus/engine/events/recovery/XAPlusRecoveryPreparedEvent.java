package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusRecoveredResource;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusRecoveryPreparedEvent extends Event<XAPlusRecoveryPreparedEvent.Handler> {

    private final Set<XAPlusRecoveredResource> recoveredResources;

    public XAPlusRecoveryPreparedEvent(Set<XAPlusRecoveredResource> recoveredResources) {
        super();
        if (recoveredResources == null) {
            throw new NullPointerException("recoveredResources is null");
        }
        this.recoveredResources = ConcurrentHashMap.newKeySet();
        this.recoveredResources.addAll(recoveredResources);
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleRecoveryPrepared(this);
    }

    public Set<XAPlusRecoveredResource> getRecoveredResources() {
        return recoveredResources;
    }

    public interface Handler {
        void handleRecoveryPrepared(XAPlusRecoveryPreparedEvent event) throws InterruptedException;
    }
}