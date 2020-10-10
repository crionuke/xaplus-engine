package org.xaplus.engine;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
public interface XAPlusFactory {
    XAPlusResource createXAPlusResource() throws XAPlusException;
}
