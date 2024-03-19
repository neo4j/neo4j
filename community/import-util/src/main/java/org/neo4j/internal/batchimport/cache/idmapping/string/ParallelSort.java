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
package org.neo4j.internal.batchimport.cache.idmapping.string;

import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import org.eclipse.collections.api.stack.primitive.MutableLongStack;
import org.eclipse.collections.impl.stack.mutable.primitive.LongArrayStack;
import org.neo4j.internal.batchimport.Utils;
import org.neo4j.internal.batchimport.cache.LongArray;
import org.neo4j.internal.batchimport.input.Group;
import org.neo4j.internal.helpers.progress.ProgressListener;

/**
 * Sorts input data by dividing up into chunks and sort each chunk in parallel. Each chunk is sorted
 * using a quick sort method, whereas the dividing of the data is first sorted using radix sort.
 */
public class ParallelSort {
    private final RadixCalculator radixCalculator;
    private final LongArray dataCache;
    private final long highestSetIndex;
    private final Tracker tracker;
    private final int threads;
    private final Radix radix;
    private final long highestSetTrackerIndex;
    private final ProgressListener progress;
    private final Comparator comparator;

    public ParallelSort(
            Radix radix,
            LongArray dataCache,
            long highestSetIndex,
            long highestSetTrackerIndex,
            Tracker tracker,
            int threads,
            ProgressListener progress,
            Comparator comparator) {
        this.highestSetTrackerIndex = highestSetTrackerIndex;
        this.progress = progress;
        this.comparator = comparator;
        this.radix = radix;
        this.radixCalculator = radix.calculator();
        this.dataCache = dataCache;
        this.highestSetIndex = highestSetIndex;
        this.tracker = tracker;
        this.threads = threads;
    }

    public synchronized SortBucket[] run() throws InterruptedException {
        SortBucket[] sortBuckets = sortRadix();
        Workers<SortWorker> sortWorkers = new Workers<>("SortWorker");
        for (int i = 0; i < threads; i++) {
            if (sortBuckets[i].count == 0) {
                break;
            }
            sortWorkers.start(new SortWorker(sortBuckets[i].baseIndex, sortBuckets[i].count));
        }
        sortWorkers.awaitAndThrowOnError();
        return sortBuckets;
    }

    private SortBucket[] sortRadix() throws InterruptedException {
        SortBucket[] sortBuckets = new SortBucket[threads];
        for (int i = 0; i < sortBuckets.length; i++) {
            sortBuckets[i] = new SortBucket();
        }
        int[] bucketRange = new int[threads];
        Workers<TrackerInitializer> initializers = new Workers<>("TrackerInitializer");
        long dataSize = highestSetTrackerIndex + 1;
        long bucketSize = dataSize / threads;
        long count = 0;
        long fullCount = 0;
        long[] radixIndexCount = radix.getRadixIndexCounts();
        for (int i = 0, threadIndex = 0; i < radixIndexCount.length && threadIndex < threads; i++) {
            if ((count + radixIndexCount[i]) > bucketSize) {
                bucketRange[threadIndex] = count == 0 ? i : i - 1;
                sortBuckets[threadIndex].baseIndex = fullCount;
                if (count != 0) {
                    sortBuckets[threadIndex].count = count;
                    fullCount += count;
                    count = radixIndexCount[i];
                } else {
                    sortBuckets[threadIndex].count = radixIndexCount[i];
                    fullCount += radixIndexCount[i];
                }
                initializers.start(new TrackerInitializer(
                        threadIndex,
                        sortBuckets[threadIndex],
                        threadIndex > 0 ? bucketRange[threadIndex - 1] : -1,
                        bucketRange[threadIndex],
                        progress));
                threadIndex++;
            } else {
                count += radixIndexCount[i];
            }
            if (threadIndex == threads - 1 || i == radixIndexCount.length - 1) {
                bucketRange[threadIndex] = radixIndexCount.length;
                sortBuckets[threadIndex].baseIndex = fullCount;
                sortBuckets[threadIndex].count = dataSize - fullCount;
                initializers.start(new TrackerInitializer(
                        threadIndex,
                        sortBuckets[threadIndex],
                        threadIndex > 0 ? bucketRange[threadIndex - 1] : -1,
                        bucketRange[threadIndex],
                        progress));
                break;
            }
        }

        // In the loop above where we split up radixes into buckets, we start one thread per bucket whose
        // job is to populate trackerCache and sortBuckets where each thread will not touch the same
        // data indexes as any other thread. Here we wait for them all to finish.
        Throwable error = initializers.await();
        long[] bucketIndex = new long[threads];
        int i = 0;
        for (TrackerInitializer initializer : initializers) {
            bucketIndex[i++] = initializer.bucketIndex;
        }
        if (error != null) {
            throw new AssertionError(
                    error.getMessage() + "\n" + dumpBuckets(sortBuckets, bucketRange, bucketIndex), error);
        }
        return sortBuckets;
    }

    private static String dumpBuckets(SortBucket[] sortBuckets, int[] bucketRange, long[] bucketIndex) {
        StringBuilder builder = new StringBuilder();
        builder.append("rangeParams:\n");
        for (SortBucket bucket : sortBuckets) {
            builder.append("  ").append(bucket).append("\n");
        }
        builder.append("bucketRange:\n");
        for (int range : bucketRange) {
            builder.append("  ").append(range).append("\n");
        }
        builder.append("bucketIndex:\n");
        for (long index : bucketIndex) {
            builder.append("  ").append(index).append("\n");
        }
        return builder.toString();
    }

    /**
     * Pluggable comparator for the comparisons that quick-sort needs in order to function.
     */
    public interface Comparator {
        /**
         * @return {@code true} if {@code left} is less than {@code pivot}.
         */
        boolean lt(long left, long pivot);

        /**
         * @return {@code true} if {@code right} is greater than or equal to {@code pivot}.
         */
        boolean ge(long right, long pivot);

        /**
         * @param dataValue the data value in the used dataCache for a given tracker index.
         * @return actual data value given the data value retrieved from the dataCache at a given index.
         * This is exposed to be able to introduce an indirection while preparing the tracker indexes
         * just like the other methods on this interface does.
         */
        long dataValue(long dataValue);
    }

    public static final Comparator DEFAULT = new Comparator() {
        @Override
        public boolean lt(long left, long pivot) {
            return Utils.unsignedCompare(left, pivot, Utils.CompareType.LT);
        }

        @Override
        public boolean ge(long right, long pivot) {
            return Utils.unsignedCompare(right, pivot, Utils.CompareType.GE);
        }

        @Override
        public long dataValue(long dataValue) {
            return dataValue;
        }
    };

    /**
     * Sorts a part of data in dataCache covered by trackerCache. Values in data cache doesn't change location,
     * instead trackerCache is updated to point to the right indexes. Only touches a designated part of trackerCache
     * so that many can run in parallel on their own part without synchronization.
     */
    private class SortWorker implements Runnable {
        private final long start;
        private final long size;
        private int threadLocalProgress;
        private final long[] pivotChoice = new long[10];
        private final ThreadLocalRandom random = ThreadLocalRandom.current();

        SortWorker(long startRange, long size) {
            this.start = startRange;
            this.size = size;
        }

        void incrementProgress(long diff) {
            threadLocalProgress += diff;
            if (threadLocalProgress
                    >= 10_000 /*reasonably big to dwarf passing a memory barrier*/) { // Update the total progress
                reportProgress();
            }
        }

        private void reportProgress() {
            progress.add(threadLocalProgress);
            threadLocalProgress = 0;
        }

        @Override
        public void run() {
            qsort(start, start + size);
            reportProgress();
        }

        private long partition(long leftIndex, long rightIndex, long pivotIndex) {
            long li = leftIndex;
            long ri = rightIndex - 2;
            long pi = pivotIndex;
            long pivot = EncodingIdMapper.clearCollision(dataCache.get(tracker.get(pi)));
            // save pivot in last index
            tracker.swap(pi, rightIndex - 1);
            long left = EncodingIdMapper.clearCollision(dataCache.get(tracker.get(li)));
            long right = EncodingIdMapper.clearCollision(dataCache.get(tracker.get(ri)));
            while (li < ri) {
                if (comparator.lt(left, pivot)) { // this value is on the correct side of the pivot, moving on
                    left = EncodingIdMapper.clearCollision(dataCache.get(tracker.get(++li)));
                } else if (comparator.ge(right, pivot)) { // this value is on the correct side of the pivot, moving on
                    right = EncodingIdMapper.clearCollision(dataCache.get(tracker.get(--ri)));
                } else { // this value is on the wrong side of the pivot, swapping
                    tracker.swap(li, ri);
                    long temp = left;
                    left = right;
                    right = temp;
                }
            }
            long partingIndex = ri;
            if (comparator.lt(right, pivot)) {
                partingIndex++;
            }
            // restore pivot
            tracker.swap(rightIndex - 1, partingIndex);
            return partingIndex;
        }

        private void qsort(long initialStart, long initialEnd) {
            final MutableLongStack stack = new LongArrayStack();
            stack.push(initialStart);
            stack.push(initialEnd);
            while (!stack.isEmpty()) {
                long end = stack.isEmpty() ? -1 : stack.pop();
                long start = stack.isEmpty() ? -1 : stack.pop();
                long diff = end - start;
                if (diff < 2) {
                    incrementProgress(2);
                    continue;
                }

                incrementProgress(1);

                // choose a random pivot between start and end
                long pivot = start + random.nextLong(diff);
                pivot = informedPivot(start, end, pivot);

                // partition, given that pivot
                pivot = partition(start, end, pivot);
                if (pivot > start) { // there are elements to left of pivot
                    stack.push(start);
                    stack.push(pivot);
                }
                if (pivot + 1 < end) { // there are elements to right of pivot
                    stack.push(pivot + 1);
                    stack.push(end);
                }
            }
        }

        private long informedPivot(long start, long end, long randomIndex) {
            if (end - start < pivotChoice.length) {
                return randomIndex;
            }

            long low = Math.max(start, randomIndex - 5);
            long high = Math.min(low + 10, end);
            int length = safeCastLongToInt(high - low);

            int j = 0;
            for (long i = low; i < high; i++, j++) {
                pivotChoice[j] = EncodingIdMapper.clearCollision(dataCache.get(tracker.get(i)));
            }
            Arrays.sort(pivotChoice, 0, length);

            long middle = pivotChoice[length / 2];
            for (long i = low; i <= high; i++) {
                if (EncodingIdMapper.clearCollision(dataCache.get(tracker.get(i))) == middle) {
                    return i;
                }
            }
            throw new IllegalStateException("The middle value somehow disappeared in front of our eyes");
        }
    }

    /**
     * Sets the initial tracker indexes pointing to data indexes. Only touches a designated part of trackerCache
     * so that many can run in parallel on their own part without synchronization.
     */
    private class TrackerInitializer implements Runnable {
        private final SortBucket sortBucket;
        private final int lowRadixRangeExclusive;
        private final int highRadixRangeInclusive;
        private final ProgressListener progress;
        private final int threadIndex;
        private long bucketIndex;

        TrackerInitializer(
                int threadIndex,
                SortBucket sortBucket,
                int lowRadixRangeExclusive,
                int highRadixRangeInclusive,
                ProgressListener progress) {
            this.threadIndex = threadIndex;
            this.sortBucket = sortBucket;
            this.lowRadixRangeExclusive = lowRadixRangeExclusive;
            this.highRadixRangeInclusive = highRadixRangeInclusive;
            this.progress = progress;
        }

        @Override
        public void run() {
            try (var localProgress = progress.threadLocalReporter()) {
                for (long i = 0; i <= highestSetIndex; i++) {
                    int rIndex = radixCalculator.radixOf(comparator.dataValue(dataCache.get(i)));
                    if (rIndex > lowRadixRangeExclusive && rIndex <= highRadixRangeInclusive) {
                        long trackerIndex = sortBucket.baseIndex + bucketIndex++;
                        assert tracker.get(trackerIndex) == -1
                                : "Overlapping buckets i:" + i + ", k:" + threadIndex + ", index:" + trackerIndex;
                        tracker.set(trackerIndex, i);
                        if (bucketIndex == sortBucket.count) {
                            sortBucket.highRadixRange = highRadixRangeInclusive;
                        }
                        localProgress.add(1);
                    }
                }
            }
        }
    }

    /**
     * Data about one bucket out of many that data gets divided into and sorted independently. After sorted {@link EncodingIdMapper#get(Object, Group)}
     * uses this information to know where to look for the real node ID for the given ID, like so:
     *
     * <ol>
     *     <li>Start from radix 0 and go through the buckets to see which one contains data about the desired radix using
     *     {@link SortBucket#highRadixRange}. The radix is retrieved by encoding the user-requested ID and calculating the radix from it
     *     ({@link RadixCalculator#radixOf(long)})</li>
     *     <li>Do a binary search given the {@link SortBucket#baseIndex} as low tracker index and {@link SortBucket#baseIndex} + {@link SortBucket#count}
     *     as high tracker index</li>
     * </ol>
     */
    static class SortBucket {
        /**
         * The high radix range (exclusive) for this bucket. The buckets are ordered so that they are checked for previous low to
         * the high range for match.
         */
        int highRadixRange;

        /**
         * The tracker index which the values matching the radix range starts at.
         */
        long baseIndex;

        /**
         * The number of values in this bucket, i.e. the tracker index range for this bucket is:
         * {@code baseIndex (inclusive)} to {@code baseIndex + count (exclusive)}.
         */
        long count;

        @Override
        public String toString() {
            return "SortBucket{" + "highRadixRange=" + highRadixRange + ", baseIndex=" + baseIndex + ", count=" + count
                    + '}';
        }
    }
}
