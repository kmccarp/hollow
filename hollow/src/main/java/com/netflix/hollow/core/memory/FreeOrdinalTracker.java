/*
 *  Copyright 2016-2021 Netflix, Inc.
 *
 *     Licensed under the Apache License, Version 2.0 (the "License");
 *     you may not use this file except in compliance with the License.
 *     You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 *
 */
package com.netflix.hollow.core.memory;

import java.util.Arrays;

/**
 * A stack of unused ordinals.<p>
 *
 * This data structure is used by the {@link ByteArrayOrdinalMap} to track and assign unused ordinals to new records.
 * 
 * The goal is to ensure the "holes" generated by removing unused ordinals during server processing are reused in subsequent cycles,
 * instead of growing the "ordinal space" indefinitely.
 *
 * @author dkoszewnik
 *
 */
public class FreeOrdinalTracker {

    private int[] freeOrdinals;
    private int size;
    private int nextEmptyOrdinal;

    public FreeOrdinalTracker() {
        this(0);
    }

    private FreeOrdinalTracker(int nextEmptyOrdinal) {
        this.freeOrdinals = new int[64];
        this.nextEmptyOrdinal = nextEmptyOrdinal;
        this.size = 0;
    }

    /**
     * @return either an ordinal which was previously deallocated, or the next empty, previously unallocated ordinal in the sequence 0-n
     */
    public int getFreeOrdinal() {
        if(size == 0) {
            return nextEmptyOrdinal++;
        }

        return freeOrdinals[--size];
    }

    /**
     * Return an ordinal to the pool after the object to which it was assigned is discarded.
     *
     * @param ordinal the ordinal
     */
    public void returnOrdinalToPool(int ordinal) {
        if(size == freeOrdinals.length) {
            freeOrdinals = Arrays.copyOf(freeOrdinals, freeOrdinals.length * 3 / 2);
        }

        freeOrdinals[size] = ordinal;
        size++;
    }

    /**
     * Specify the next ordinal to return after the reusable pool is exhausted
     *
     * @param nextEmptyOrdinal the next empty ordinal
     */
    public void setNextEmptyOrdinal(int nextEmptyOrdinal) {
        this.nextEmptyOrdinal = nextEmptyOrdinal;
    }

    /**
     * Ensure that all future ordinals are returned in ascending order.
     */
    public void sort() {
        Arrays.sort(freeOrdinals, 0, size);
        reverseFreeOrdinalPool();
    }

    /**
     * Focus returned ordinal holes in as few shards as possible.
     * Within each shard, return ordinals in ascending order.
     */
    public void sort(int numShards) {
        int shardNumberMask = numShards - 1;
        Shard[] shards = new Shard[numShards];
        for(int i = 0;i < shards.length;i++) {
            shards[i] = new Shard();
        }

        for(int i = 0;i < size;i++) {
            shards[freeOrdinals[i] & shardNumberMask].freeOrdinalCount++;
        }

        Shard[] orderedShards = Arrays.copyOf(shards, shards.length);
        Arrays.sort(orderedShards, (s1, s2) -> s2.freeOrdinalCount - s1.freeOrdinalCount);

        for(int i = 1;i < numShards;i++) {
            orderedShards[i].currentPos = orderedShards[i - 1].currentPos + orderedShards[i - 1].freeOrdinalCount;
        }

        /// each shard will receive the ordinals in ascending order.
        Arrays.sort(freeOrdinals, 0, size);

        int[] newFreeOrdinals = new int[freeOrdinals.length];
        for(int i=0;i<size;i++) {
            Shard shard = shards[freeOrdinals[i] & shardNumberMask];
            newFreeOrdinals[shard.currentPos] = freeOrdinals[i];
            shard.currentPos++;
        }

        freeOrdinals = newFreeOrdinals;

        reverseFreeOrdinalPool();
    }

    private static class Shard {
        private int freeOrdinalCount;
        private int currentPos;
    }

    private void reverseFreeOrdinalPool() {
        int midpoint = size / 2;
        for(int i=0;i<midpoint;i++) {
            int temp = freeOrdinals[i];
            freeOrdinals[i] = freeOrdinals[size-i-1];
            freeOrdinals[size-i-1] = temp;
        }
    }
    
    /**
     * Resets the FreeOrdinalTracker to its initial state.
     */
    public void reset() {
        size = 0;
        nextEmptyOrdinal = 0;
    }

}
