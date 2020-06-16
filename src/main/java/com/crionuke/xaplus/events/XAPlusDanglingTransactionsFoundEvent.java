package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;
import com.crionuke.xaplus.XAPlusXid;

import java.util.Collections;
import java.util.Map;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusDanglingTransactionsFoundEvent extends Event<XAPlusDanglingTransactionsFoundEvent.Handler> {

    private final Map<String, Map<XAPlusXid, Boolean>> danglingTransactions;

    public XAPlusDanglingTransactionsFoundEvent(Map<String, Map<XAPlusXid, Boolean>> danglingTransactions) {
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

    public Map<String, Map<XAPlusXid, Boolean>> getDanglingTransactions() {
        return danglingTransactions;
    }

    public interface Handler {
        void handleDanglingTransactionFound(XAPlusDanglingTransactionsFoundEvent event) throws InterruptedException;
    }
}