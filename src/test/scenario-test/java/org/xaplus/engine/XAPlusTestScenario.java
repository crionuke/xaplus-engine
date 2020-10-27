package org.xaplus.engine;

class XAPlusTestScenario {
    volatile boolean prepareException;
    volatile boolean readyException;
    volatile boolean commitException;
    volatile boolean rollbackException;
    volatile boolean doneException;
    volatile boolean retryException;
    volatile boolean absentException;

    XAPlusTestScenario() {
        this.prepareException = true;
        this.readyException = true;
        this.commitException = true;
        this.rollbackException = true;
        this.doneException = true;
        this.retryException = true;
        this.absentException = true;
    }
}
