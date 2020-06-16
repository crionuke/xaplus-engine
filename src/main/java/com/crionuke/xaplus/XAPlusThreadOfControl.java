package com.crionuke.xaplus;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
@Component
final class XAPlusThreadOfControl {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusThreadOfControl.class);

    private final com.crionuke.xaplus.XAPlusProperties XAPlusProperties;
    private final ThreadOfControl threadOfControl;

    XAPlusThreadOfControl(XAPlusProperties XAPlusProperties) {
        this.XAPlusProperties = XAPlusProperties;
        threadOfControl = new ThreadOfControl();
    }

    XAPlusThreadContext getThreadContext() {
        return threadOfControl.get();
    }

    private class ThreadOfControl extends ThreadLocal<XAPlusThreadContext> {

        @Override
        protected XAPlusThreadContext initialValue() {
            return new XAPlusThreadContext(XAPlusProperties);
        }
    }
}
