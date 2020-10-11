package org.xaplus.engine.exceptions;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public class XAPlusSystemException extends Exception {

    public XAPlusSystemException(String message) {
        super(message);
    }

    public XAPlusSystemException(Throwable cause) {
        super(cause);
    }
}
