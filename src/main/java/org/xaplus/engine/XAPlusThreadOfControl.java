package org.xaplus.engine;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusThreadOfControl {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusThreadOfControl.class);

    private final ThreadOfControl threadOfControl;

    XAPlusThreadOfControl() {
        threadOfControl = new ThreadOfControl();
    }

    XAPlusThreadContext getThreadContext() {
        return threadOfControl.get();
    }

    private class ThreadOfControl extends ThreadLocal<XAPlusThreadContext> {

        @Override
        protected XAPlusThreadContext initialValue() {
            return new XAPlusThreadContext();
        }
    }
}
