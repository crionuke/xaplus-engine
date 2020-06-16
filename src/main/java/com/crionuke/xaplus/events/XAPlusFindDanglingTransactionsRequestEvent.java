package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusFindDanglingTransactionsRequestEvent extends Event<XAPlusFindDanglingTransactionsRequestEvent.Handler> {

    public XAPlusFindDanglingTransactionsRequestEvent() {
        super();
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleFindDanglingTransactionsRequest(this);
    }

    public interface Handler {
        void handleFindDanglingTransactionsRequest(XAPlusFindDanglingTransactionsRequestEvent event) throws InterruptedException;
    }
}