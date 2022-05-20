/**
 * Copyright 2022 Yahoo Inc. All rights reserved.
 */
package org.apache.datasketches.count;

import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * 
 */
public class CountConfigTest {

    @Test
    public void testEquals() {
        CountConfig c = new CountConfig(1, 2, 0);
        CountConfig c2 = new CountConfig(1, 2, 0);
        CountConfig c3 = new CountConfig(2, 1, 0);
        CountConfig c4 = new CountConfig(1, 2, 1);
        
        assertTrue(c.equals(c));
        assertFalse(c.equals(null));
        assertTrue(c.equals(c2));
        assertFalse(c.equals(c3));
        assertFalse(c.equals(c4));
        
    }
}
