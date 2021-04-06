package org.xaplus.engine.events.journal;

import com.crionuke.bolts.Event;
import org.xaplus.engine.XAPlusUid;
import org.xaplus.engine.XAPlusXid;

import java.util.Collections;
import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusDanglingTransactionsFoundEvent extends Event<XAPlusDanglingTransactionsFoundEvent.Handler> {

    private final Map<XAPlusUid, Boolean> danglingTransactions;

    public XAPlusDanglingTransactionsFoundEvent(Map<XAPlusUid, Boolean> danglingTransactions) {
        super();
        if (danglingTransactions == null) {
            throw new NullPointerException("danglingTransactions is null");
        }
        this.danglingTransactions = Collections.unmodifiableMap(danglingTransactions);
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleDanglingTransactionFound(this);
    }

    public Map<XAPlusUid, Boolean> getDanglingTransactions() {
        return danglingTransactions;
    }

    public interface Handler {
        void handleDanglingTransactionFound(XAPlusDanglingTransactionsFoundEvent event) throws InterruptedException;
    }
}