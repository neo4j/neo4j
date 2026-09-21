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
package org.neo4j.io.memory;

import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.util.Arrays;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * Port of the JDK java.util.DualPivotQuicksort but works over ByteBuffers instead of arrays of primitives. Currently
 * only works for sorting long values.
 */
@SuppressWarnings("StatementWithEmptyBody")
class ByteBufferQuicksort {

    /*
    PLEASE NOTE: this code probably doesn't align with the style quide for the codebase but I tried to keep it as
    close to the naming/style/format of the original java.util.DualPivotQuicksort that was adapted to use LongBuffer
    instead of long[] to make checking the changes easier
     */

    private ByteBufferQuicksort() {}

    /**
     * Max long count to use mixed insertion sort.
     */
    private static final int MAX_MIXED_INSERTION_SORT_SIZE = 65;

    /**
     * Max long count to use insertion sort.
     */
    private static final int MAX_INSERTION_SORT_SIZE = 44;

    /**
     * Min long count to try merging of runs.
     */
    static final int MIN_TRY_MERGE_SIZE = 4 << 10;

    /**
     * Min size of the first run to continue with scanning.
     */
    private static final int MIN_FIRST_RUN_SIZE = 16;

    /**
     * Min factor for the first runs to continue scanning.
     */
    private static final int MIN_FIRST_RUNS_FACTOR = 7;

    /**
     * Max capacity of the index array for tracking runs.
     */
    private static final int MAX_RUN_CAPACITY = 5 << 10;

    /**
     * Threshold of mixed insertion sort is incremented by this value.
     */
    private static final int DELTA = 3 << 1;

    /**
     * Max recursive partitioning depth before using heap sort.
     */
    private static final int MAX_RECURSION_DEPTH = 64 * DELTA;

    /**
     * Sorts the specified range of the ByteBuffer using Dual-Pivot Quicksort.
     *
     * @param buffer the buffer to be sorted
     */
    static void sort(ByteBuffer buffer, MemoryTracker memoryTracker) {
        if (!buffer.hasRemaining()) {
            // nothing to do
            return;
        }

        Preconditions.checkArgument(buffer.remaining() % 8 == 0, "The buffer must only contain longs");
        var longs = buffer.asLongBuffer();
        sort(longs, 0, 0, longs.limit(), memoryTracker);
    }

    /**
     * Sorts the specified buffer using the Dual-Pivot Quicksort and/or
     * other sorts in special-cases, possibly with parallel partitions.
     *
     * @param a    the buffer to be sorted
     * @param bits the combination of recursion depth and bit flag, where
     *             the right bit "0" indicates that buffer is the leftmost part
     * @param low  the index of the first long, inclusive, to be sorted
     * @param high the index of the last long, exclusive, to be sorted
     */
    private static void sort(LongBuffer a, int bits, int low, int high, MemoryTracker memoryTracker) {
        while (true) {
            int end = high - 1, size = high - low;

            /*
             * Run mixed insertion sort on small non-leftmost parts.
             */
            if (size < MAX_MIXED_INSERTION_SORT_SIZE + bits && (bits & 1) > 0) {
                mixedInsertionSort(a, low, high - 3 * ((size >> 5) << 3), high);
                return;
            }

            /*
             * Invoke insertion sort on small leftmost part.
             */
            if (size < MAX_INSERTION_SORT_SIZE) {
                insertionSort(a, low, high);
                return;
            }

            /*
             * Check if the whole buffer or large non-leftmost
             * parts are nearly sorted and then merge runs.
             */
            if ((bits == 0 || size > MIN_TRY_MERGE_SIZE && (bits & 1) > 0)
                    && tryMergeRuns(a, low, size, memoryTracker)) {
                return;
            }

            /*
             * Switch to heap sort if execution
             * time is becoming quadratic.
             */
            if ((bits += DELTA) > MAX_RECURSION_DEPTH) {
                heapSort(a, low, high);
                return;
            }

            /*
             * Use an inexpensive approximation of the golden ratio
             * to select five sample longs and determine pivots.
             */
            int step = (size >> 3) * 3 + 3;

            /*
             * Five longs around (and including) the central long
             * will be used for pivot selection as described below. The
             * unequal choice of spacing these longs was empirically
             * determined to work well on a wide variety of inputs.
             */
            int e1 = low + step;
            int e5 = end - step;
            int e3 = (e1 + e5) >>> 1;
            int e2 = (e1 + e3) >>> 1;
            int e4 = (e3 + e5) >>> 1;
            long a3 = a.get(e3);

            /*
             * Sort these longs in place by the combination
             * of 4-long sorting network and insertion sort.
             *
             *    5 ------o-----------o------------
             *            |           |
             *    4 ------|-----o-----o-----o------
             *            |     |           |
             *    2 ------o-----|-----o-----o------
             *                  |     |
             *    1 ------------o-----o------------
             */
            if (a.get(e5) < a.get(e2)) {
                long t = a.get(e5);
                a.put(e5, a.get(e2));
                a.put(e2, t);
            }
            if (a.get(e4) < a.get(e1)) {
                long t = a.get(e4);
                a.put(e4, a.get(e1));
                a.put(e1, t);
            }
            if (a.get(e5) < a.get(e4)) {
                long t = a.get(e5);
                a.put(e5, a.get(e4));
                a.put(e4, t);
            }
            if (a.get(e2) < a.get(e1)) {
                long t = a.get(e2);
                a.put(e2, a.get(e1));
                a.put(e1, t);
            }
            if (a.get(e4) < a.get(e2)) {
                long t = a.get(e4);
                a.put(e4, a.get(e2));
                a.put(e2, t);
            }

            if (a3 < a.get(e2)) {
                if (a3 < a.get(e1)) {
                    a.put(e3, a.get(e2));
                    a.put(e2, a.get(e1));
                    a.put(e1, a3);
                } else {
                    a.put(e3, a.get(e2));
                    a.put(e2, a3);
                }
            } else if (a3 > a.get(e4)) {
                if (a3 > a.get(e5)) {
                    a.put(e3, a.get(e4));
                    a.put(e4, a.get(e5));
                    a.put(e5, a3);
                } else {
                    a.put(e3, a.get(e4));
                    a.put(e4, a3);
                }
            }

            // Pointers
            int lower = low; // The index of the last long of the left part
            int upper = end; // The index of the first long of the right part

            /*
             * Partitioning with 2 pivots in case of different longs.
             */
            if (a.get(e1) < a.get(e2) && a.get(e2) < a.get(e3) && a.get(e3) < a.get(e4) && a.get(e4) < a.get(e5)) {

                /*
                 * Use the first and fifth of the five sorted longs as
                 * the pivots. These values are inexpensive approximation
                 * of tertiles. Note, that pivot1 < pivot2.
                 */
                long pivot1 = a.get(e1);
                long pivot2 = a.get(e5);

                /*
                 * The first and the last longs to be sorted are moved
                 * to the locations formerly occupied by the pivots. When
                 * partitioning is completed, the pivots are swapped back
                 * into their final positions, and excluded from the next
                 * subsequent sorting.
                 */
                a.put(e1, a.get(lower));
                a.put(e5, a.get(upper));

                /*
                 * Skip longs, which are less or greater than the pivots.
                 */
                while (a.get(++lower) < pivot1)
                    ;
                while (a.get(--upper) > pivot2)
                    ;

                /*
                 * Backward 3-interval partitioning
                 *
                 *   left part                 central part          right part
                 * +------------------------------------------------------------+
                 * |  < pivot1  |   ?   |  pivot1 <= && <= pivot2  |  > pivot2  |
                 * +------------------------------------------------------------+
                 *             ^       ^                            ^
                 *             |       |                            |
                 *           lower     k                          upper
                 *
                 * Invariants:
                 *
                 *              all in (low, lower] < pivot1
                 *    pivot1 <= all in (k, upper)  <= pivot2
                 *              all in [upper, end) > pivot2
                 *
                 * Pointer k is the last index of ?-part
                 */
                for (int ignored = --lower, k = ++upper; --k > lower; ) {
                    long ak = a.get(k);

                    if (ak < pivot1) { // Move a.get(k) to the left side
                        while (lower < k) {
                            if (a.get(++lower) >= pivot1) {
                                if (a.get(lower) > pivot2) {
                                    a.put(k, a.get(--upper));
                                    a.put(upper, a.get(lower));
                                } else {
                                    a.put(k, a.get(lower));
                                }
                                a.put(lower, ak);
                                break;
                            }
                        }
                    } else if (ak > pivot2) { // Move a.get(k) to the right side
                        a.put(k, a.get(--upper));
                        a.put(upper, ak);
                    }
                }

                /*
                 * Swap the pivots into their final positions.
                 */
                a.put(low, a.get(lower));
                a.put(lower, pivot1);
                a.put(end, a.get(upper));
                a.put(upper, pivot2);

                sort(a, bits | 1, lower + 1, upper, memoryTracker);
                sort(a, bits | 1, upper + 1, high, memoryTracker);
            } else { // Use single pivot in case of many equal longs

                /*
                 * Use the third of the five sorted longs as the pivot.
                 * This value is inexpensive approximation of the median.
                 */
                long pivot = a.get(e3);

                /*
                 * The first long to be sorted is moved to the
                 * location formerly occupied by the pivot. After
                 * completion of partitioning the pivot is swapped
                 * back into its final position, and excluded from
                 * the next subsequent sorting.
                 */
                a.put(e3, a.get(lower));

                /*
                 * Traditional 3-way (Dutch National Flag) partitioning
                 *
                 *   left part                 central part    right part
                 * +------------------------------------------------------+
                 * |   < pivot   |     ?     |   == pivot   |   > pivot   |
                 * +------------------------------------------------------+
                 *              ^           ^                ^
                 *              |           |                |
                 *            lower         k              upper
                 *
                 * Invariants:
                 *
                 *   all in (low, lower] < pivot
                 *   all in (k, upper)  == pivot
                 *   all in [upper, end] > pivot
                 *
                 * Pointer k is the last index of ?-part
                 */
                for (int k = ++upper; --k > lower; ) {
                    long ak = a.get(k);

                    if (ak != pivot) {
                        a.put(k, pivot);

                        if (ak < pivot) { // Move a.get(k) to the left side
                            while (a.get(++lower) < pivot)
                                ;

                            if (a.get(lower) > pivot) {
                                a.put(--upper, a.get(lower));
                            }
                            a.put(lower, ak);
                        } else { // ak > pivot - Move a.get(k) to the right side
                            a.put(--upper, ak);
                        }
                    }
                }

                /*
                 * Swap the pivot into its final position.
                 */
                a.put(low, a.get(lower));
                a.put(lower, pivot);

                /*
                 * Sort the right part, excluding known pivot. All longs from the central part are equal and
                 * therefore already sorted.
                 */
                sort(a, bits | 1, upper, high, memoryTracker);
            }
            high = lower; // Iterate along the left part
        }
    }

    /**
     * Sorts the specified range of the buffer using mixed insertion sort.
     * <br>
     * Mixed insertion sort is combination of simple insertion sort,
     * pin insertion sort and pair insertion sort.
     * <br>
     * In the context of Dual-Pivot Quicksort, the pivot long
     * from the left part plays the role of sentinel, because it
     * is less than any longs from the given part. Therefore,
     * expensive check of the left range can be skipped on each
     * iteration unless it is the leftmost call.
     * <br>
     *
     * @param a    the buffer to be sorted
     * @param low  the index of the first long, inclusive, to be sorted
     * @param end  the index of the last long for simple insertion sort
     * @param high the index of the last long, exclusive, to be sorted
     */
    private static void mixedInsertionSort(LongBuffer a, int low, int end, int high) {
        if (end == high) {

            /*
             * Invoke simple insertion sort on tiny buffer.
             */
            for (int i; ++low < end; ) {
                long ai = a.get(i = low);

                while (ai < a.get(--i)) {
                    a.put(i + 1, a.get(i));
                }
                a.put(i + 1, ai);
            }
        } else {

            /*
             * Start with pin insertion sort on small part.
             *
             * Pin insertion sort is extended simple insertion sort.
             * The main idea of this sort is to put longs larger
             * than an long called pin to the end of buffer (the
             * proper area for such longs). It avoids expensive
             * movements of these longs through the whole buffer.
             */
            long pin = a.get(end);

            for (int i, p = high; ++low < end; ) {
                long ai = a.get(i = low);

                if (ai < a.get(i - 1)) { // Small long

                    /*
                     * Insert small long into sorted part.
                     */
                    a.put(i, a.get(--i));

                    while (ai < a.get(--i)) {
                        a.put(i + 1, a.get(i));
                    }
                    a.put(i + 1, ai);
                } else if (p > i && ai > pin) { // Large long

                    /*
                     * Find long smaller than pin.
                     */
                    while (a.get(--p) > pin)
                        ;

                    /*
                     * Swap it with large long.
                     */
                    if (p > i) {
                        ai = a.get(p);
                        a.put(p, a.get(i));
                    }

                    /*
                     * Insert small long into sorted part.
                     */
                    while (ai < a.get(--i)) {
                        a.put(i + 1, a.get(i));
                    }
                    a.put(i + 1, ai);
                }
            }

            /*
             * Continue with pair insertion sort on remain part.
             */
            for (int i; low < high; ++low) {
                long a1 = a.get(i = low), a2 = a.get(++low);

                /*
                 * Insert two longs per iteration: at first, insert the
                 * larger long and then insert the smaller long, but
                 * from the position where the larger long was inserted.
                 */
                if (a1 > a2) {

                    while (a1 < a.get(--i)) {
                        a.put(i + 2, a.get(i));
                    }
                    a.put(++i + 1, a1);

                    while (a2 < a.get(--i)) {
                        a.put(i + 1, a.get(i));
                    }
                    a.put(i + 1, a2);
                } else if (a1 < a.get(i - 1)) {

                    while (a2 < a.get(--i)) {
                        a.put(i + 2, a.get(i));
                    }
                    a.put(++i + 1, a2);

                    while (a1 < a.get(--i)) {
                        a.put(i + 1, a.get(i));
                    }
                    a.put(i + 1, a1);
                }
            }
        }
    }

    /**
     * Sorts the specified range of the buffer using insertion sort.
     *
     * @param a    the buffer to be sorted
     * @param low  the index of the first long, inclusive, to be sorted
     * @param high the index of the last long, exclusive, to be sorted
     */
    private static void insertionSort(LongBuffer a, int low, int high) {
        for (int i, k = low; ++k < high; ) {
            long ai = a.get((i = k));

            if (ai < a.get((i - 1))) {
                while (--i >= low && ai < a.get(i)) {
                    a.put(i + 1, a.get(i));
                }
                a.put(i + 1, ai);
            }
        }
    }

    /**
     * Sorts the specified range of the buffer using heap sort.
     *
     * @param a    the buffer to be sorted
     * @param low  the index of the first long, inclusive, to be sorted
     * @param high the index of the last long, exclusive, to be sorted
     */
    private static void heapSort(LongBuffer a, int low, int high) {
        for (int k = (low + high) >>> 1; k > low; ) {
            pushDown(a, --k, a.get(k), low, high);
        }
        while (--high > low) {
            long max = a.get(low);
            pushDown(a, low, a.get(high), low, high);
            a.put(high, max);
        }
    }

    /**
     * Pushes specified long down during heap sort.
     *
     * @param a     the given buffer
     * @param p     the start index
     * @param value the given long
     * @param low   the index of the first long, inclusive, to be sorted
     * @param high  the index of the last long, exclusive, to be sorted
     */
    private static void pushDown(LongBuffer a, int p, long value, int low, int high) {
        for (int k; ; a.put(p, a.get(p = k))) {
            k = (p << 1) - low + 2; // Index of the right child

            if (k > high) {
                break;
            }
            if (k == high || a.get(k) < a.get(k - 1)) {
                --k;
            }
            if (a.get(k) <= value) {
                break;
            }
        }
        a.put(p, value);
    }

    /**
     * Tries to sort the specified range of the buffer.
     *
     * @param a    the buffer to be sorted
     * @param low  the index of the first long to be sorted
     * @param size the number of longs to sort
     * @return true if finally sorted, false otherwise
     */
    private static boolean tryMergeRuns(LongBuffer a, int low, int size, MemoryTracker memoryTracker) {

        /*
         * The run buffer is constructed only if initial runs are
         * long enough to continue, run[i] then holds start index
         * of the i-th sequence of longs in non-descending order.
         */
        int[] run = null;
        int high = low + size;
        int count = 1, last = low;

        /*
         * Identify all possible runs.
         */
        for (int k = low + 1; k < high; ) {

            /*
             * Find the end index of the current run.
             */
            if (a.get(k - 1) < a.get(k)) {

                // Identify ascending sequence
                while (++k < high && a.get(k - 1) <= a.get(k))
                    ;
            } else if (a.get(k - 1) > a.get(k)) {

                // Identify descending sequence
                while (++k < high && a.get(k - 1) >= a.get(k))
                    ;

                // Reverse into ascending order
                for (int i = last - 1, j = k; ++i < --j && a.get(i) > a.get(j); ) {
                    long ai = a.get(i);
                    a.put(i, a.get(j));
                    a.put(j, ai);
                }
            } else { // Identify constant sequence
                for (long ak = a.get(k); ++k < high && ak == a.get(k); )
                    ;

                if (k < high) {
                    continue;
                }
            }

            /*
             * Check special cases.
             */
            if (run == null) {
                if (k == high) {

                    /*
                     * The buffer is monotonous sequence,
                     * and therefore already sorted.
                     */
                    return true;
                }

                if (k - low < MIN_FIRST_RUN_SIZE) {

                    /*
                     * The first run is too small
                     * to proceed with scanning.
                     */
                    return false;
                }

                run = new int[((size >> 10) | 0x7F) & 0x3FF];
                run[0] = low;
            } else if (a.get(last - 1) > a.get(last)) {

                if (count > (k - low) >> MIN_FIRST_RUNS_FACTOR) {

                    /*
                     * The first runs are not long
                     * enough to continue scanning.
                     */
                    return false;
                }

                if (++count == MAX_RUN_CAPACITY) {

                    /*
                     * buffer is not highly structured.
                     */
                    return false;
                }

                if (count == run.length) {

                    /*
                     * Increase capacity of index buffer.
                     */
                    run = Arrays.copyOf(run, count << 1);
                }
            }
            run[count] = (last = k);
        }

        /*
         * Merge runs of highly structured buffer.
         */
        if (count > 1) {
            var tempBuffer = ByteBuffers.allocate(size * Long.BYTES, a.order(), memoryTracker);
            try {
                mergeRuns(a, tempBuffer.asLongBuffer(), low, 1, run, 0, count);
            } finally {
                ByteBuffers.releaseBuffer(tempBuffer, memoryTracker);
            }
        }
        return true;
    }

    /**
     * Merges the specified runs.
     *
     * @param a      the source buffer
     * @param b      the temporary buffer used in merging
     * @param offset the start index in the source, inclusive
     * @param aim    specifies merging: to source ( > 0), buffer ( < 0) or any ( == 0)
     * @param run    the start indexes of the runs, inclusive
     * @param lo     the start index of the first run, inclusive
     * @param hi     the start index of the last run, inclusive
     * @return the destination where runs are merged
     */
    private static LongBuffer mergeRuns(LongBuffer a, LongBuffer b, int offset, int aim, int[] run, int lo, int hi) {
        if (hi - lo == 1) {
            if (aim >= 0) {
                return a;
            }
            for (int i = run[hi], j = i - offset, low = run[lo]; i > low; b.put(--j, a.get(--i)))
                ;
            return b;
        }

        /*
         * Split into approximately equal parts.
         */
        int mi = lo, rmi = (run[lo] + run[hi]) >>> 1;
        while (run[++mi + 1] <= rmi)
            ;

        /*
         * Merge the left and right parts.
         */
        var a1 = mergeRuns(a, b, offset, -aim, run, lo, mi);
        var a2 = mergeRuns(a, b, offset, 0, run, mi, hi);

        var dst = a1 == a ? b : a;

        int k = a1 == a ? run[lo] - offset : run[lo];
        int lo1 = a1 == b ? run[lo] - offset : run[lo];
        int hi1 = a1 == b ? run[mi] - offset : run[mi];
        int lo2 = a2 == b ? run[mi] - offset : run[mi];
        int hi2 = a2 == b ? run[hi] - offset : run[hi];

        mergeParts(dst, k, a1, lo1, hi1, a2, lo2, hi2);
        return dst;
    }

    /**
     * Merges the sorted parts.
     *
     * @param dst the destination where parts are merged
     * @param k   the start index of the destination, inclusive
     * @param a1  the first part
     * @param lo1 the start index of the first part, inclusive
     * @param hi1 the end index of the first part, exclusive
     * @param a2  the second part
     * @param lo2 the start index of the second part, inclusive
     * @param hi2 the end index of the second part, exclusive
     */
    private static void mergeParts(
            LongBuffer dst, int k, LongBuffer a1, int lo1, int hi1, LongBuffer a2, int lo2, int hi2) {
        /*
         * Merge small parts sequentially.
         */
        while (lo1 < hi1 && lo2 < hi2) {
            dst.put(k++, a1.get(lo1) < a2.get(lo2) ? a1.get(lo1++) : a2.get(lo2++));
        }
        if (dst != a1 || k < lo1) {
            while (lo1 < hi1) {
                dst.put(k++, a1.get(lo1++));
            }
        }
        if (dst != a2 || k < lo2) {
            while (lo2 < hi2) {
                dst.put(k++, a2.get(lo2++));
            }
        }
    }
}
