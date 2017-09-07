package com.chauncy.simpledb.systemtest;

import com.chauncy.simpledb.Database;
import org.junit.Before;


/**
 * Base class for all SimpleDb test classes.
 *
 * @author nizam
 */
public class SimpleDbTestBase {
    /**
     * Reset the database before each test is run.
     */
    @Before
    public void setUp() throws Exception {
        Database.reset();
    }

}
