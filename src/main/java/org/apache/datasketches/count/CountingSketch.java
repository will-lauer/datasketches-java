/**
 * Copyright 2022 Yahoo Inc. All rights reserved.
 */
package org.apache.datasketches.count;

/**
 * A counting sketch that collects a number of items and then can return the estimated
 * number of times a specific item was seen. 
 *
 * @param <T> The type of item to count. Must have a reasonable implementation of {@link Object#hashCode()}
 */
public interface CountingSketch<T> {
    /**
     * Add the item to the sketch, with an implicit weight of 1
     * 
     * @param item The item to add
     */
    void update(T item);
    
    /**
     * Add the item to the sketch with the specified weight. Weights can be negative,
     * indicating that the item is being removed from the sketch instead of added.
     * 
     * @param item The item to add
     * @param weight The weight associated with the item.
     */
    void update(T item, long weight);
    
    /**
     * Get the estimated count (weight) for the specified item.
     * 
     * @param item The item to check
     * @return The estimated weight
     */
    long getEstimate(T item);
    
    /**
     * THe number of hashes
     * @return The number of hashes
     */
    int getNumHashes();
    
    /**
     * The number of buckets
     * @return The number of buckets
     */
    int getNumBuckets();
    
    /**
     * Returns the total weight of the sketch
     * @return the total weight of the sketch
     */
    long getTotalWeight();
}
