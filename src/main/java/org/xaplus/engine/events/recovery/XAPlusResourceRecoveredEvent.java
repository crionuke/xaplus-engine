package org.xaplus.engine.events.recovery;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusXid;

import java.util.Collections;
import java.util.Set;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusResourceRecoveredEvent extends Event<XAPlusResourceRecoveredEvent.Handler> {

    private final String uniqueName;
    private final Set<XAPlusXid> recoveredXids;

    public XAPlusResourceRecoveredEvent(String uniqueName, Set<XAPlusXid> recoveredXids) {
        super();
        if (uniqueName == null) {
            throw new NullPointerException("uniqueName is null");
        }
        if (recoveredXids == null) {
            throw new NullPointerException("recoveredXids is null");
        }
        this.recoveredXids = Collections.unmodifiableSet(recoveredXids);
        this.uniqueName = uniqueName;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleResourceRecovered(this);
    }

    public String getUniqueName() {
        return uniqueName;
    }

    public Set<XAPlusXid> getRecoveredXids() {
        return recoveredXids;
    }

    public interface Handler {
        void handleResourceRecovered(XAPlusResourceRecoveredEvent event) throws InterruptedException;
    }
}