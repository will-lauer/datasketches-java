/**
 * Copyright 2022 Yahoo Inc. All rights reserved.
 */
package org.apache.datasketches.count;

/**
 * @param <T> 
 * 
 */
public class CountMinSketch<T extends Number> implements CountingSketch<T> {

    private CountConfig config;
    private long[] table;
    private long totalWeight;

    public CountMinSketch(CountConfig config) {
        this.config = config;
        this.table = new long[config.getNumHashes() * config.getNumBuckets()];
    }
    
    public CountMinSketch(int numHashes, int numBuckets) {
        this(new CountConfig(numHashes, numBuckets, System.currentTimeMillis()));
    }
    
    public CountMinSketch(int numHashes, int numBuckets, long randomSeed) {
        this(new CountConfig(numHashes, numBuckets, randomSeed));
    }
    
    @Override
    public void update(T item) {
        update(item, 1);
    }

    @Override
    public void update(T item, long weight) {
        for (int ii = 0; ii < config.getNumHashes(); ++ii) {
            int bucket = (int) (hashItem(item, ii) % config.getNumBuckets());
            table[(config.getNumHashes() * ii) + bucket] += weight;
        }
        totalWeight += weight;
    }

    @Override
    public long getEstimate(T item) {
        long estimate = Long.MAX_VALUE;
        
        for (int ii = 0; ii < config.getNumHashes(); ++ii) {
            int bucket = (int) (hashItem(item, ii) % config.getNumBuckets());
            estimate = Math.min(estimate, table[(config.getNumHashes() * ii) + bucket]);
        }
        
        return estimate;
    }

    @Override
    public int getNumHashes() {
        return config.getNumHashes();
    }

    @Override
    public int getNumBuckets() {
        return config.getNumBuckets();
    }
    
    protected long hashItem(T item, int hashNum) {
        long hash = ((config.getHashCoefA()[hashNum] * item.longValue() + config.getHashCoefB()[hashNum]) % config.getPrime());
        
        return hash >= 0 ? hash : hash + config.getPrime();
    }

    @Override
    public long getTotalWeight() {
        return totalWeight;
    }

}
