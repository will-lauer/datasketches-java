/**
 * Copyright 2022 Yahoo Inc. All rights reserved.
 */
package org.apache.datasketches.count;

import java.util.Random;

/**
 * @param <T> 
 * 
 */
public class CountMinSketch<T extends Number> implements CountingSketch<T> {
    
    /**
     * The prime being used as an exponent to compute the mersenne prime
     * used in the hashing. must be one of 2,3,5,7,13,17,19,31...
     */
    public static final long MERSENNE_EXP = 31;
    public static final long MERSENNE_PRIME = (1 << MERSENNE_EXP) - 1;
    
    private long[] hashCoefB;
    private long[] hashCoefA;
    private long[] table;
    private long totalWeight;
    private int numHashes;
    private int numBuckets;

    public CountMinSketch(int numHashes, int numBuckets) {
        this(numHashes, numBuckets, System.currentTimeMillis());
    }
    
    public CountMinSketch(int numHashes, int numBuckets, long randomSeed) {
        Random rnd = new Random(randomSeed);
        
        this.hashCoefA = rnd.longs(numHashes).toArray();
        this.hashCoefB = rnd.longs(numHashes).toArray();
        
        this.numBuckets = numBuckets;
        this.numHashes = numHashes;
        this.table = new long[numHashes * numBuckets];
    }

    @Override
    public void update(T item) {
        update(item, 1);
    }

    @Override
    public void update(T item, long weight) {
        for (int ii = 0; ii < numHashes; ++ii) {
            int bucket = (int) (hashItem(item, ii) % numBuckets);
            if (bucket < 0) {
                bucket += numBuckets;
            }
            table[(numHashes * ii) + bucket] += weight;
        }
        totalWeight += weight;
    }

    @Override
    public long getEstimate(T item) {
        long estimate = Long.MAX_VALUE;
        
        for (int ii = 0; ii < numHashes; ++ii) {
            int bucket = (int) (hashItem(item, ii) % numBuckets);
            if (bucket < 0) {
                bucket += numBuckets;
            }
            estimate = Math.min(estimate, table[(numHashes * ii) + bucket]);
        }
        
        return estimate;
    }

    @Override
    public int getNumHashes() {
        return numHashes;
    }

    @Override
    public int getNumBuckets() {
        return numBuckets;
    }
    
    protected long hashItem(T item, int hashNum) {
        long hash = ((hashCoefA[hashNum] * item.longValue() + hashCoefB[hashNum]) % MERSENNE_PRIME );
        
        return hash >= 0 ? hash : hash + MERSENNE_PRIME;
    }

    @Override
    public long getTotalWeight() {
        return totalWeight;
    }

}
