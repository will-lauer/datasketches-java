/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.yahoo.sketches.theta;

import static com.yahoo.sketches.QuickSelect.selectExcludingZeros;
import static com.yahoo.sketches.theta.CompactSketch.compactCache;
import static com.yahoo.sketches.theta.PreambleUtil.COMPACT_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.ORDERED_FLAG_MASK;
import static com.yahoo.sketches.theta.PreambleUtil.PREAMBLE_LONGS_BYTE;
import static com.yahoo.sketches.theta.PreambleUtil.THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.UNION_THETA_LONG;
import static com.yahoo.sketches.theta.PreambleUtil.clearEmpty;
import static com.yahoo.sketches.theta.PreambleUtil.extractCurCount;
import static com.yahoo.sketches.theta.PreambleUtil.extractFamilyID;
import static com.yahoo.sketches.theta.PreambleUtil.extractFlags;
import static com.yahoo.sketches.theta.PreambleUtil.extractLgArrLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractPreLongs;
import static com.yahoo.sketches.theta.PreambleUtil.extractSeedHash;
import static com.yahoo.sketches.theta.PreambleUtil.extractSerVer;
import static com.yahoo.sketches.theta.PreambleUtil.extractThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.extractUnionThetaLong;
import static com.yahoo.sketches.theta.PreambleUtil.insertUnionThetaLong;
import static java.lang.Math.min;

import com.yahoo.memory.Memory;
import com.yahoo.memory.MemoryRequestServer;
import com.yahoo.memory.WritableMemory;
import com.yahoo.sketches.Family;
import com.yahoo.sketches.HashOperations;
import com.yahoo.sketches.ResizeFactor;
import com.yahoo.sketches.SketchesArgumentException;
import com.yahoo.sketches.Util;

/**
 * Shared code for the HeapUnion and DirectUnion implementations.
 *
 * @author Lee Rhodes
 * @author Kevin Lang
 */
final class UnionImpl extends Union {

  /**
   * Although the gadget object is initially an UpdateSketch, in the context of a Union it is used
   * as a specialized buffer that happens to leverage much of the machinery of an UpdateSketch.
   * However, in this context some of the key invariants of the sketch algorithm are intentionally
   * violated as an optimization. As a result this object can not be considered as an UpdateSketch
   * and should never be exported as an UpdateSketch. It's internal state is not necessarily
   * finalized and may contain garbage. Also its internal concept of "nominal entries" or "k" can
   * be meaningless. It is private for very good reasons.
   */
  private final UpdateSketch gadget_;
  private final short seedHash_; //eliminates having to compute the seedHash on every update.
  private long unionThetaLong_; //when on-heap, this is the only copy
  private boolean unionEmpty_;  //when on-heap, this is the only copy

  private UnionImpl(final UpdateSketch gadget, final long seed) {
    gadget_ = gadget;
    seedHash_ = computeSeedHash(seed);
  }

  /**
   * Construct a new Union SetOperation on the java heap.
   * Called by SetOperationBuilder.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @return instance of this sketch
   */
  static UnionImpl initNewHeapInstance(final int lgNomLongs, final long seed, final float p,
      final ResizeFactor rf) {
    final UpdateSketch gadget = //create with UNION family
        new HeapQuickSelectSketch(lgNomLongs, seed, p, rf, true);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = gadget.getThetaLong();
    unionImpl.unionEmpty_ = gadget.isEmpty();
    return unionImpl;
  }

  /**
   * Construct a new Direct Union in the off-heap destination Memory.
   * Called by SetOperationBuilder.
   *
   * @param lgNomLongs <a href="{@docRoot}/resources/dictionary.html#lgNomLogs">See lgNomLongs</a>.
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @param p <a href="{@docRoot}/resources/dictionary.html#p">See Sampling Probability, <i>p</i></a>
   * @param rf <a href="{@docRoot}/resources/dictionary.html#resizeFactor">See Resize Factor</a>
   * @param memReqSvr a given instance of a MemoryRequestServer
   * @param dstMem the given Memory object destination. It will be cleared prior to use.
   * @return this class
   */
  static UnionImpl initNewDirectInstance(
      final int lgNomLongs,
      final long seed,
      final float p,
      final ResizeFactor rf,
      final MemoryRequestServer memReqSvr,
      final WritableMemory dstMem) {
    final UpdateSketch gadget = //create with UNION family
        new DirectQuickSelectSketch(lgNomLongs, seed, p, rf, memReqSvr, dstMem, true);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = gadget.getThetaLong();
    unionImpl.unionEmpty_ = gadget.isEmpty();
    return unionImpl;
  }

  /**
   * Heapify a Union from a Memory Union object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory Union object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl heapifyInstance(final Memory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = HeapQuickSelectSketch.heapifyInstance(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  /**
   * Fast-wrap a Union object around a Union Memory object containing data.
   * This does NO validity checking of the given Memory.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl fastWrap(final Memory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketchR.fastReadOnlyWrap(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  /**
   * Fast-wrap a Union object around a Union Memory object containing data.
   * This does NO validity checking of the given Memory.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl fastWrap(final WritableMemory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketch.fastWritableWrap(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  /**
   * Wrap a Union object around a Union Memory object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl wrapInstance(final Memory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketchR.readOnlyWrap(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  /**
   * Wrap a Union object around a Union Memory object containing data.
   * Called by SetOperation.
   * @param srcMem The source Memory object.
   * <a href="{@docRoot}/resources/dictionary.html#mem">See Memory</a>
   * @param seed <a href="{@docRoot}/resources/dictionary.html#seed">See seed</a>
   * @return this class
   */
  static UnionImpl wrapInstance(final WritableMemory srcMem, final long seed) {
    Family.UNION.checkFamilyID(extractFamilyID(srcMem));
    final UpdateSketch gadget = DirectQuickSelectSketch.writableWrap(srcMem, seed);
    final UnionImpl unionImpl = new UnionImpl(gadget, seed);
    unionImpl.unionThetaLong_ = extractUnionThetaLong(srcMem);
    unionImpl.unionEmpty_ = PreambleUtil.isEmpty(srcMem);
    return unionImpl;
  }

  @Override
  public CompactSketch getResult() {
    return getResult(true, null);
  }

  @Override
  public CompactSketch getResult(final boolean dstOrdered, final WritableMemory dstMem) {
    final int gadgetCurCount = gadget_.getRetainedEntries(true);
    final int k = 1 << gadget_.getLgNomLongs();
    final long[] gadgetCacheCopy =
        (gadget_.hasMemory()) ? gadget_.getCache() : gadget_.getCache().clone();

    //Pull back to k
    final long curGadgetThetaLong = gadget_.getThetaLong();
    final long adjGadgetThetaLong = (gadgetCurCount > k)
        ? selectExcludingZeros(gadgetCacheCopy, gadgetCurCount, k + 1) : curGadgetThetaLong;

    //Finalize Theta and curCount
    final long unionThetaLong = (gadget_.hasMemory())
        ? gadget_.getMemory().getLong(UNION_THETA_LONG) : unionThetaLong_;

    final long minThetaLong = min(min(curGadgetThetaLong, adjGadgetThetaLong), unionThetaLong);
    final int curCountOut = (minThetaLong < curGadgetThetaLong)
        ? HashOperations.count(gadgetCacheCopy, minThetaLong)
        : gadgetCurCount;

    //Compact the cache
    final long[] compactCacheOut =
        compactCache(gadgetCacheCopy, curCountOut, minThetaLong, dstOrdered);
    final boolean empty = gadget_.isEmpty() && unionEmpty_;
    return createCompactSketch(
        compactCacheOut, empty, seedHash_, curCountOut, minThetaLong, dstOrdered, dstMem);
  }

  @Override
  public void reset() {
    gadget_.reset();
    unionThetaLong_ = gadget_.getThetaLong();
    unionEmpty_ = gadget_.isEmpty();
  }

  @Override
  public byte[] toByteArray() {
    final byte[] gadgetByteArr = gadget_.toByteArray();
    final WritableMemory mem = WritableMemory.wrap(gadgetByteArr);
    insertUnionThetaLong(mem, unionThetaLong_);
    if (gadget_.isEmpty() != unionEmpty_) {
      clearEmpty(mem);
      unionEmpty_ = false;
    }
    return gadgetByteArr;
  }

  @Override
  public boolean isSameResource(final Memory that) {
    return (gadget_ instanceof DirectQuickSelectSketchR)
        ? gadget_.getMemory().isSameResource(that) : false;
  }

  @Override
  public void update(final Sketch sketchIn) { //Only valid for theta Sketches using SerVer = 3
    //UNION Empty Rule: AND the empty states.

    if ((sketchIn == null) || sketchIn.isEmpty()) {
      //null and empty is interpreted as (Theta = 1.0, count = 0, empty = T).  Nothing changes
      return;
    }
    //sketchIn is valid and not empty
    Util.checkSeedHashes(seedHash_, sketchIn.getSeedHash());
    Sketch.checkSketchAndMemoryFlags(sketchIn);

    unionThetaLong_ = min(min(unionThetaLong_, sketchIn.getThetaLong()), gadget_.getThetaLong()); //Theta rule
    unionEmpty_ = false;
    final int curCountIn = sketchIn.getRetainedEntries(true);
    if (curCountIn > 0) {
      if (sketchIn.isOrdered()) { //Only true if Compact. Use early stop
        //Ordered, thus compact
        if (sketchIn.hasMemory()) {
          final Memory skMem = ((CompactSketch) sketchIn).getMemory();
          final int preambleLongs = skMem.getByte(PREAMBLE_LONGS_BYTE) & 0X3F;
          for (int i = 0; i < curCountIn; i++ ) {
            final int offsetBytes = (preambleLongs + i) << 3;
            final long hashIn = skMem.getLong(offsetBytes);
            if (hashIn >= unionThetaLong_) { break; } // "early stop"
            gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
          }
        }
        else { //sketchIn is on the Java Heap or has array
          final long[] cacheIn = sketchIn.getCache(); //not a copy!
          for (int i = 0; i < curCountIn; i++ ) {
            final long hashIn = cacheIn[i];
            if (hashIn >= unionThetaLong_) { break; } // "early stop"
            gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
          }
        }
      } //End ordered, compact
      else { //either not-ordered compact or Hash Table form. A HT may have dirty values.
        final long[] cacheIn = sketchIn.getCache(); //if off-heap this will be a copy
        final int arrLongs = cacheIn.length;
        for (int i = 0, c = 0; (i < arrLongs) && (c < curCountIn); i++ ) {
          final long hashIn = cacheIn[i];
          if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) { continue; } //rejects dirty values
          gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
          c++; //ensures against invalid state inside the incoming sketch
        }
      }
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule with gadget
    if (gadget_.hasMemory()) {
      final WritableMemory wmem = (WritableMemory)gadget_.getMemory();
      PreambleUtil.insertUnionThetaLong(wmem, unionThetaLong_);
      PreambleUtil.clearEmpty(wmem);
    }
  }

  @Override
  public void update(final Memory skMem) {
    if (skMem == null) { return; }
    final int cap = (int) skMem.getCapacity();
    if (cap < 16) { return; } //empty or garbage
    final int serVer = extractSerVer(skMem);
    final int fam = extractFamilyID(skMem);

    if (serVer == 3) { //The OpenSource sketches (Aug 4, 2015)
      if ((fam < 1) || (fam > 3)) {
        throw new SketchesArgumentException(
            "Family must be Alpha, QuickSelect, or Compact: " + Family.idToFamily(fam));
      }
      processVer3(skMem);
      return;
    }

    if (fam != 3) { //In older sketches this family was called the SetSketch
      throw new SketchesArgumentException(
          "Family must be old SET_SKETCH (now COMPACT) = 3: " + Family.idToFamily(fam));
    }

    if (serVer == 2) { //older Sketch, which is compact and ordered
      Util.checkSeedHashes(seedHash_, (short)extractSeedHash(skMem));
      processVer2(skMem);
      return;
    }

    if (serVer == 1) { //much older Sketch, which is compact and ordered
      processVer1(skMem, cap);
      return;
    }

    throw new SketchesArgumentException("SerVer is unknown: " + serVer);
  }

  //Has seedHash, p, could have 0 entries & theta < 1.0,
  //could be unordered, ordered, compact, or not compact, size >= 16,
  //could be Alpha, QuickSelect, or Compact.
  private void processVer3(final Memory skMem) {
    final int preLongs = extractPreLongs(skMem);

    if (preLongs == 1) { //we know cap >= 16
      //This test requires compact, ordered, notEmpty, ReadOnly, LE, seedHash is OK;
      // OR the above and the SI bit is set
      if (SingleItemSketch.testPre0SeedHash(skMem.getLong(0), seedHash_)) {
        final long hash = skMem.getLong(8);
        update(hash); //a hash < 1 will be rejected later
        return;
      }
      return; //empty
    }

    Util.checkSeedHashes(seedHash_, (short)extractSeedHash(skMem));

    final int curCountIn;
    final long thetaLongIn;

    if (preLongs == 2) { //exact mode
      curCountIn = extractCurCount(skMem);
      if (curCountIn == 0) { return; } //should be > 0, but if it is return empty anyway.
      thetaLongIn = Long.MAX_VALUE;
    }

    else { //prelongs == 3
      //curCount may be 0 (e.g., from intersection); but sketch cannot be empty.
      curCountIn = extractCurCount(skMem);
      thetaLongIn = extractThetaLong(skMem);
    }

    unionThetaLong_ = min(min(unionThetaLong_, thetaLongIn), gadget_.getThetaLong()); //theta rule
    unionEmpty_ = false;
    final int flags = extractFlags(skMem);
    final boolean ordered = (flags & ORDERED_FLAG_MASK) != 0;
    if (ordered) { //must be compact

      for (int i = 0; i < curCountIn; i++ ) {
        final int offsetBytes = (preLongs + i) << 3;
        final long hashIn = skMem.getLong(offsetBytes);
        if (hashIn >= unionThetaLong_) { break; } // "early stop"
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }

    else { //not-ordered, could be compact or hash-table form
      final boolean compact = (flags & COMPACT_FLAG_MASK) != 0;
      final int size = (compact) ? curCountIn : 1 << extractLgArrLongs(skMem);

      for (int i = 0; i < size; i++ ) {
        final int offsetBytes = (preLongs + i) << 3;
        final long hashIn = skMem.getLong(offsetBytes);
        if ((hashIn <= 0L) || (hashIn >= unionThetaLong_)) { continue; }
        gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
      }
    }

    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //sync thetaLongs

    if (gadget_.hasMemory()) {
      final WritableMemory wmem = (WritableMemory)gadget_.getMemory();
      PreambleUtil.insertUnionThetaLong(wmem, unionThetaLong_);
      PreambleUtil.clearEmpty(wmem);
    }
  }

  //has seedHash and p, could have 0 entries & theta,
  // can only be compact, ordered, size >= 8
  private void processVer2(final Memory skMem) {
    final int preLongs = extractPreLongs(skMem);

    if (preLongs == 1) { //does not change anything, return empty
      return;
    }

    Util.checkSeedHashes(seedHash_, (short)extractSeedHash(skMem));

    final int curCountIn;
    final long thetaLongIn;

    if (preLongs == 2) { //exact mode, not empty, cannot be a set operation
      curCountIn = extractCurCount(skMem);
      if (curCountIn == 0) { return; } //should be > 0, but if it is return empty anyway.
      thetaLongIn = Long.MAX_VALUE;
    }

    else { //prelongs == 3
      //curCount may be 0 (e.g., from intersection); but sketch cannot be empty.
      curCountIn = extractCurCount(skMem);
      thetaLongIn = extractThetaLong(skMem);
    }

    unionThetaLong_ = min(min(unionThetaLong_, thetaLongIn), gadget_.getThetaLong()); //Theta rule
    unionEmpty_ = false;

    for (int i = 0; i < curCountIn; i++ ) {
      final int offsetBytes = (preLongs + i) << 3;
      final long hashIn = skMem.getLong(offsetBytes);
      if (hashIn >= unionThetaLong_) { break; } // "early stop"
      gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
    }

    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong());

    if (gadget_.hasMemory()) {
      final WritableMemory wmem = (WritableMemory)gadget_.getMemory();
      PreambleUtil.insertUnionThetaLong(wmem, unionThetaLong_);
      PreambleUtil.clearEmpty(wmem);
    }
  }

  //no seedHash, assumes given seed is correct. No p, no empty flag, no concept of direct
  // can only be compact, ordered, size > 24
  private void processVer1(final Memory skMem, final int cap) {
    final long thetaLongIn = skMem.getLong(THETA_LONG);
    final int curCountIn = extractCurCount(skMem);
    if ((cap <= 24) || ((curCountIn == 0) && (unionThetaLong_ == Long.MAX_VALUE))) {
      return; //empty
    }

    unionThetaLong_ = min(min(unionThetaLong_, thetaLongIn), gadget_.getThetaLong()); //Theta rule
    unionEmpty_ = false;

    final int preLongs = 3;
    for (int i = 0; i < curCountIn; i++ ) {
      final int offsetBytes = (preLongs + i) << 3;
      final long hashIn = skMem.getLong(offsetBytes);
      if (hashIn >= unionThetaLong_) { break; } // "early stop"
      gadget_.hashUpdate(hashIn); //backdoor update, hash function is bypassed
    }
    unionThetaLong_ = min(unionThetaLong_, gadget_.getThetaLong()); //Theta rule
    if (gadget_.hasMemory()) {
      final WritableMemory wmem = (WritableMemory)gadget_.getMemory();
      PreambleUtil.insertUnionThetaLong(wmem, unionThetaLong_);
      PreambleUtil.clearEmpty(wmem);
    }
  }

  @Override
  public void update(final long datum) {
    gadget_.update(datum);
  }

  @Override
  public void update(final double datum) {
    gadget_.update(datum);
  }

  @Override
  public void update(final String datum) {
    gadget_.update(datum);
  }

  @Override
  public void update(final byte[] data) {
    gadget_.update(data);
  }

  @Override
  public void update(final char[] data) {
    gadget_.update(data);
  }

  @Override
  public void update(final int[] data) {
    gadget_.update(data);
  }

  @Override
  public void update(final long[] data) {
    gadget_.update(data);
  }

  //Restricted

  @Override
  long[] getCache() {
    return gadget_.getCache();
  }

  @Override
  int getRetainedEntries(final boolean valid) {
    return gadget_.getRetainedEntries(valid);
  }

  @Override
  short getSeedHash() {
    return gadget_.getSeedHash();
  }

  @Override
  long getThetaLong() {
    return min(unionThetaLong_, gadget_.getThetaLong());
  }

  @Override
  boolean isEmpty() {
    return gadget_.isEmpty() && unionEmpty_;
  }

}
