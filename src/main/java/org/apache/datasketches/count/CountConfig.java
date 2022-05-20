/**
 * Copyright 2022 Yahoo Inc. All rights reserved.
 */
package org.apache.datasketches.count;

import java.util.Objects;
import java.util.Random;

/**
 * 
 */
public class CountConfig {
    /**
     * The prime being used as an exponent to compute the mersenne prime
     * (2^n -1) used in the hashing. must be one of 2,3,5,7,13,17,19,31...
     */
    public static final long MERSENNE_EXP = 31;

    private final long[] hashCoefA;
    private final long[] hashCoefB;
    private final long prime;
    
    private final int numBuckets;
    private final int numHashes;
    
    public CountConfig(int numHashes, int numBuckets, long randomSeed) {
        Random rnd = new Random(randomSeed);
        
        this.prime =  (1 << MERSENNE_EXP) - 1;
        this.hashCoefA = rnd.longs(numHashes, 1, prime).toArray();
        this.hashCoefB = rnd.longs(numHashes, 1, prime).toArray();
        
        this.numBuckets = numBuckets;
        this.numHashes = numHashes;
    }

    public long[] getHashCoefA() {
        return hashCoefA;
    }

    public long[] getHashCoefB() {
        return hashCoefB;
    }

    public long getPrime() {
        return prime;
    }

    public static long getMersenneExp() {
        return MERSENNE_EXP;
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public int getNumHashes() {
        return numHashes;
    }    
    
    @Override
    public boolean equals(Object obj) {
        if (obj == this) { 
            return true;
        } else if (obj == null) {
            return false;
        } else if (!(obj instanceof CountConfig)) {
            return false;
        } else {
            CountConfig that = (CountConfig) obj;
            return this.numHashes == that.numHashes &&
                    this.numBuckets == that.numBuckets &&
                    this.prime == that.prime &&
                    Objects.deepEquals(this.hashCoefA, that.hashCoefA) &&
                    Objects.deepEquals(this.hashCoefB, that.hashCoefB);
        }
    }
}
