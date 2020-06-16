package com.crionuke.xaplus.events;

import com.crionuke.bolts.Event;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public final class XAPlusTickEvent extends Event<XAPlusTickEvent.Handler> {

    private final int index;

    public XAPlusTickEvent(int index) {
        super();
        this.index = index;
    }

    @Override
    public void handle(Handler handler) throws InterruptedException {
        handler.handleTick(this);
    }

    public int getIndex() {
        return index;
    }

    public interface Handler {
        void handleTick(XAPlusTickEvent event) throws InterruptedException;
    }
}