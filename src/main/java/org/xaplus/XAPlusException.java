package org.xaplus;

import javax.transaction.xa.XAException;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public class XAPlusException extends XAException {

    public XAPlusException(String s) {
        super(s);
    }

    public XAPlusException(int errcode) {
        super(errcode);
    }
}
