package org.xaplus.engine;

import org.junit.Before;
import org.junit.Test;

public class XAPlusThreadContextTest extends XAPlusTest {

    XAPlusThreadContext threadContext;

    @Before
    public void beforeTest() {
        createXAPlusComponents(XA_PLUS_RESOURCE_1);
        threadContext = new XAPlusThreadContext();
    }

    @Test(expected = NullPointerException.class)
    public void testWithNullTransaction() {
        threadContext.setTransaction(null);
    }

    @Test
    public void testEmptyThreadContext() {
        assertFalse(threadContext.hasTransaction());
        assertNull(threadContext.getTransaction());
    }

    @Test
    public void testThreadContext() {
        XAPlusTransaction transaction = createTransaction(XA_PLUS_RESOURCE_1, XA_PLUS_RESOURCE_1);
        threadContext.setTransaction(transaction);
        assertTrue(threadContext.hasTransaction());
        assertEquals(transaction, threadContext.getTransaction());
        threadContext.removeTransaction();
        assertNull(threadContext.getTransaction());
        assertFalse(threadContext.hasTransaction());
    }
}
