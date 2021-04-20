package org.xaplus.engine;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XAPlusPropertiesTest {
    static private final Logger logger = LoggerFactory.getLogger(XAPlusPropertiesTest.class);

    @Test(expected = IllegalArgumentException.class)
    public void testServerIdIsNull() {
        try {
            new XAPlusProperties(null, 128, 60, 60, 0);
        } catch (IllegalArgumentException e) {
            logger.info(e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testServerIdTooLong() {
        try {
            new XAPlusProperties("1234567890123456789012345678901234567890123456789012_", 128, 60, 60, 0);
        } catch (IllegalArgumentException e) {
            logger.info(e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongQueueSize() {
        try {
            new XAPlusProperties("stub", 0, 60, 60, 0);
        } catch (IllegalArgumentException e) {
            logger.info(e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongTransactionsTimeout() {
        try {
            new XAPlusProperties("stub", 128, 0, 60, 0);
        } catch (IllegalArgumentException e) {
            logger.info(e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongRecoveryTimeout() {
        try {
            new XAPlusProperties("stub", 128, 60, 0, 0);
        } catch (IllegalArgumentException e) {
            logger.info(e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testWrongRecoveryPeriodInSeconds() {
        try {
            new XAPlusProperties("stub", 128, 60, 60, -1);
        } catch (IllegalArgumentException e) {
            logger.info(e.getMessage());
            throw e;
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAllExceptions() {
        try {
            new XAPlusProperties(null, 0, 0, 0, -1);
        } catch (IllegalArgumentException e) {
            logger.info(e.getMessage());
            throw e;
        }
    }
}
