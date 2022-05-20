/**
 * Copyright 2022 Yahoo Inc. All rights reserved.
 */
package org.apache.datasketches.count;

/**
 * A basic CountMin sketch that can be used to estimate the weight of an 
 * item after ingesting a stream of items and their associated weights. 
 * The current implementation assumes that that the items being ingested are
 * Numbers, and extracts their value as a long.
 * 
 * @param <T> The type of 
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
    
    /*
     * @see CountingSketch#update(Object))
     */
    @Override
    public void update(T item) {
        update(item, 1);
    }

    /*
     * @see CountingSketch#update(Object, long))
     */
    @Override
    public void update(T item, long weight) {
        for (int ii = 0; ii < config.getNumHashes(); ++ii) {
            int bucket = (int) (hashItem(item, ii) % config.getNumBuckets());
            table[(config.getNumHashes() * ii) + bucket] += weight;
        }
        totalWeight += weight;
    }

    
    /*
     * @see CountingSketch#getEstimate
     */
    @Override
    public long getEstimate(T item) {
        long estimate = Long.MAX_VALUE;
        
        for (int ii = 0; ii < config.getNumHashes(); ++ii) {
            int bucket = (int) (hashItem(item, ii) % config.getNumBuckets());
            estimate = Math.min(estimate, table[(config.getNumHashes() * ii) + bucket]);
        }
        
        return estimate;
    }

    /*
     * @see CountingSketch#getNumHashes
     */
    @Override
    public int getNumHashes() {
        return config.getNumHashes();
    }

    /*
     * @see CountingSketch@getNumBuckets
     */
    @Override
    public int getNumBuckets() {
        return config.getNumBuckets();
    }
    
    /*
     * @see CountingSketch#getTotalWeight
     */
    @Override
    public long getTotalWeight() {
        return totalWeight;
    }

    /**
     * Compute the Nth hash of the specified item.
     * @param item the Item
     * @param hashNum which hash to compute
     * @return the hash value
     */
    protected long hashItem(T item, int hashNum) {
        long hash = ((config.getHashCoefA()[hashNum] * item.longValue() + config.getHashCoefB()[hashNum]) % config.getPrime());
        
        return hash >= 0 ? hash : hash + config.getPrime();
    }
    
    /**
     * Returns the suggested number buckets necessary to acheive
     * the specified relative error.
     * 
     * @param relativeError
     * @return the suggested number of buckets
     */
    public static int suggestNumBuckets(double relativeError) {
        return (int) Math.ceil(Math.E / relativeError);
    }

    /**
     * Returns the suggested number of hashes necessary to acheive
     * the specified probability that an estimate meets the 
     * expected relative error.
     * 
     * @param probability the desired probability
     * @return the necessary number of buckets
     */
    public static int suggestNumHashes(double probability) {
        return (int) Math.ceil(Math.log(1.0/(1.0-probability)));
    }
}
