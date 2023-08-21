/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.consistency.checking.cache;

import static org.neo4j.consistency.checking.cache.CacheSlots.ID_SLOT_SIZE;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.neo4j.consistency.checking.ByteArrayBitsManipulator;
import org.neo4j.internal.batchimport.cache.ByteArray;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.util.concurrent.Futures;

/**
 * Simply combining a {@link ByteArray} with {@link ByteArrayBitsManipulator}, so that each byte[] index can be split up into
 * slots, i.e. holding multiple values for space efficiency and convenience.
 */
class PackedMultiFieldCache {
    private final ByteArray array;
    private ByteArrayBitsManipulator slots;
    private long[] initValues;

    PackedMultiFieldCache(ByteArray array, int... slotSizes) {
        this.array = array;
        setSlotSizes(slotSizes);
    }

    void put(long index, long... values) {
        for (int i = 0; i < values.length; i++) {
            slots.set(array, index, i, values[i]);
        }
    }

    void put(long index, int slot, long value) {
        slots.set(array, index, slot, value);
    }

    long get(long index, int slot) {
        if (index < array.length()) {
            return slots.get(array, index, slot);
        }
        return initValues[slot];
    }

    void setSlotSizes(int... slotSizes) {
        this.slots = new ByteArrayBitsManipulator(slotSizes);
        this.initValues = getInitVals(slotSizes);
    }

    void clearParallel(int numThreads) {
        long length = array.length();
        List<Callable<Void>> tasks = new ArrayList<>();
        long numPerThread = length / numThreads;
        long prevLowIndex = 0;
        for (int i = 0; i < numThreads; i++) {
            long lowIndex = prevLowIndex;
            long highIndex = i == numThreads - 1 ? length : lowIndex + numPerThread;
            tasks.add(() -> {
                for (long index = lowIndex; index < highIndex; index++) {
                    clear(index);
                }
                return null;
            });
            prevLowIndex = highIndex;
        }
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        try {
            Futures.getAllResults(executor.invokeAll(tasks));
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            executor.shutdown();
        }
    }

    void clear(long index) {
        put(index, initValues);
    }

    private static long[] getInitVals(int[] slotSizes) {
        long[] initVals = new long[slotSizes.length];
        for (int i = 0; i < initVals.length; i++) {
            initVals[i] = isId(slotSizes, i) ? Record.NO_NEXT_RELATIONSHIP.intValue() : 0;
        }
        return initVals;
    }

    private static boolean isId(int[] slotSizes, int i) {
        return slotSizes[i] >= ID_SLOT_SIZE;
    }
}
