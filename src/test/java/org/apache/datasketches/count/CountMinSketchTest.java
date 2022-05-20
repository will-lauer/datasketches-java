/**
 * Copyright 2022 Yahoo Inc. All rights reserved.
 */
package org.apache.datasketches.count;

import org.testng.annotations.Test;

import java.util.Random;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * 
 */
public class CountMinSketchTest {

    @Test
    public void basicTest() {
        CountingSketch<Long> s = new CountMinSketch<>(3, 20);
        
        Random rnd = new Random(0);
        
        rnd.longs(1000, 1, 50).forEach(l -> s.update(l));
        
        assertEquals(s.getTotalWeight(), 1000);
        
        for (long ii = 0; ii< 50; ++ii) {
            System.out.println(s.getEstimate(ii));
        }
    }
    
    @Test
    public void validateBucketSuggestion() {
        assertEquals(CountMinSketch.suggestNumBuckets(.2),14);
        assertEquals(CountMinSketch.suggestNumBuckets(.1),28);
        assertEquals(CountMinSketch.suggestNumBuckets(.01),272);
    }
    
    @Test
    public void validateHashSuggestion() {
        // 1 stddev
        assertEquals(CountMinSketch.suggestNumHashes(.682689492), 2);
        // 2 stddev 
        assertEquals(CountMinSketch.suggestNumHashes(.954499736), 4);
        // 3 stddev
        assertEquals(CountMinSketch.suggestNumHashes(.997300204), 6);
    }

}
