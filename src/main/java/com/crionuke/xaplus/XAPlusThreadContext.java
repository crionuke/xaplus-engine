package com.crionuke.xaplus;

/**
 * @author Kirill Byvshev (k@byv.sh)
 * @since 1.0.0
 */
class XAPlusThreadContext {
    private final XAPlusProperties properties;

    private volatile XAPlusTransaction transaction;

    XAPlusThreadContext(XAPlusProperties properties) {
        this.properties = properties;
    }

    XAPlusTransaction getTransaction() {
        return transaction;
    }

    void setTransaction(XAPlusTransaction transaction) {
        if (transaction == null) {
            throw new NullPointerException("transaction is null");
        }
        this.transaction = transaction;
    }

    void clearTransaction() {
        transaction = null;
    }

    boolean hasTransaction() {
        return transaction != null;
    }
}
