/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.Utils;
import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;

import static org.neo4j.unsafe.impl.batchimport.Utils.safeCastLongToInt;
import static org.neo4j.unsafe.impl.batchimport.cache.idmapping.string.EncodingIdMapper.clearCollision;

/**
 * Sorts input data by dividing up into chunks and sort each chunk in parallel. Each chunk is sorted
 * using a quick sort method, whereas the dividing of the data is first sorted using radix sort.
 */
public class ParallelSort
{
    private final int[] radixIndexCount;
    private final RadixCalculator radixCalculator;
    private final LongArray dataCache;
    private final long highestSetIndex;
    private final IntArray tracker;
    private final int threads;
    private long[][] sortBuckets;
    private final ProgressListener progress;
    private final Comparator comparator;

    public ParallelSort( Radix radix, LongArray dataCache, long highestSetIndex,
            IntArray tracker, int threads, ProgressListener progress, Comparator comparator )
    {
        this.progress = progress;
        this.comparator = comparator;
        this.radixIndexCount = radix.getRadixIndexCounts();
        this.radixCalculator = radix.calculator();
        this.dataCache = dataCache;
        this.highestSetIndex = highestSetIndex;
        this.tracker = tracker;
        this.threads = threads;
    }

    public synchronized long[][] run() throws InterruptedException
    {
        int[][] sortParams = sortRadix();
        int threadsNeeded = 0;
        for ( int i = 0; i < threads; i++ )
        {
            if ( sortParams[i][1] == 0 )
            {
                break;
            }
            threadsNeeded++;
        }

        Workers<SortWorker> sortWorkers = new Workers<>( "SortWorker" );
        progress.started( "SORT" );
        for ( int i = 0; i < threadsNeeded; i++ )
        {
            if ( sortParams[i][1] == 0 )
            {
                break;
            }
            sortWorkers.start( new SortWorker( sortParams[i][0], sortParams[i][1] ) );
        }
        try
        {
            sortWorkers.awaitAndThrowOnError();
        }
        finally
        {
            progress.done();
        }
        return sortBuckets;
    }

    private int[][] sortRadix() throws InterruptedException
    {
        int[][] rangeParams = new int[threads][2];
        int[] bucketRange = new int[threads];
        Workers<TrackerInitializer> initializers = new Workers<>( "TrackerInitializer" );
        sortBuckets = new long[threads][2];
        long dataSize = highestSetIndex+1;
        int bucketSize = safeCastLongToInt( dataSize / threads );
        int count = 0, fullCount = 0;
        progress.started( "SPLIT" );
        for ( int i = 0, threadIndex = 0; i < radixIndexCount.length && threadIndex < threads; i++ )
        {
            if ( (count + radixIndexCount[i]) > bucketSize )
            {
                bucketRange[threadIndex] = count == 0 ? i : i - 1;
                rangeParams[threadIndex][0] = fullCount;
                if ( count != 0 )
                {
                    rangeParams[threadIndex][1] = count;
                    fullCount += count;
                    progress.add( count );
                    count = radixIndexCount[i];
                }
                else
                {
                    rangeParams[threadIndex][1] = radixIndexCount[i];
                    fullCount += radixIndexCount[i];
                    progress.add( radixIndexCount[i] );
                }
                initializers.start( new TrackerInitializer( threadIndex, rangeParams[threadIndex],
                        threadIndex > 0 ? bucketRange[threadIndex-1] : -1, bucketRange[threadIndex],
                        sortBuckets[threadIndex] ) );
                threadIndex++;
            }
            else
            {
                count += radixIndexCount[i];
            }
            if ( threadIndex == threads - 1 || i == radixIndexCount.length -1 )
            {
                bucketRange[threadIndex] = radixIndexCount.length;
                rangeParams[threadIndex][0] = fullCount;
                rangeParams[threadIndex][1] = safeCastLongToInt( dataSize - fullCount );
                initializers.start( new TrackerInitializer( threadIndex, rangeParams[threadIndex],
                        threadIndex > 0 ? bucketRange[threadIndex-1] : -1, bucketRange[threadIndex],
                        sortBuckets[threadIndex] ) );
                break;
            }
        }
        progress.done();

        // In the loop above where we split up radixes into buckets, we start one thread per bucket whose
        // job is to populate trackerCache and sortBuckets where each thread will not touch the same
        // data indexes as any other thread. Here we wait for them all to finish.
        Throwable error = initializers.await();
        int[] bucketIndex = new int[threads];
        int i = 0;
        for ( TrackerInitializer initializer : initializers )
        {
            bucketIndex[i++] = initializer.bucketIndex;
        }
        if ( error != null )
        {
            throw new AssertionError( error.getMessage() + "\n" + dumpBuckets( rangeParams, bucketRange, bucketIndex ),
                    error );
        }
        return rangeParams;
    }

    private String dumpBuckets( int[][] rangeParams, int[] bucketRange, int[] bucketIndex )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "rangeParams:\n" );
        for ( int[] range : rangeParams )
        {
            builder.append( "  " ).append( Arrays.toString( range ) ).append( "\n" );
        }
        builder.append( "bucketRange:\n" );
        for ( int range : bucketRange )
        {
            builder.append( "  " ).append( range ).append( "\n" );
        }
        builder.append( "bucketIndex:\n" );
        for ( int index : bucketIndex )
        {
            builder.append( "  " ).append( index ).append( "\n" );
        }
        return builder.toString();
    }

    /**
     * Pluggable comparator for the comparisons that quick-sort needs in order to function.
     */
    public interface Comparator
    {
        /**
         * @return {@code true} if {@code left} is less than {@code pivot}.
         */
        boolean lt( long left, long pivot );

        /**
         * @return {@code true} if {@code right} is greater than or equal to {@code pivot}.
         */
        boolean ge( long right, long pivot );
    }

    public static final Comparator DEFAULT = new Comparator()
    {
        @Override
        public boolean lt( long left, long pivot )
        {
            return Utils.unsignedCompare( left, pivot, CompareType.LT );
        }

        @Override
        public boolean ge( long right, long pivot )
        {
            return Utils.unsignedCompare( right, pivot, CompareType.GE );
        }
    };

    /**
     * Sorts a part of data in dataCache covered by trackerCache. Values in data cache doesn't change location,
     * instead trackerCache is updated to point to the right indexes. Only touches a designated part of trackerCache
     * so that many can run in parallel on their own part without synchronization.
     */
    private class SortWorker implements Runnable
    {
        private final int start, size;
        private int threadLocalProgress;
        private final long[] pivotChoice = new long[10];
        private final Random random = ThreadLocalRandom.current();

        SortWorker( int startRange, int size )
        {
            this.start = startRange;
            this.size = size;
        }

        void incrementProgress( int diff )
        {
            threadLocalProgress += diff;
            if ( threadLocalProgress == 10_000 /*reasonably big to dwarf passing a memory barrier*/ )
            {   // Update the total progress
                reportProgress();
            }
        }

        private void reportProgress()
        {
            progress.add( threadLocalProgress );
            threadLocalProgress = 0;
        }

        @Override
        public void run()
        {
            qsort( start, start + size );
            reportProgress();
        }

        private int partition( int leftIndex, int rightIndex, int pivotIndex )
        {
            int li = leftIndex, ri = rightIndex - 2, pi = pivotIndex;
            long pivot = clearCollision( dataCache.get( tracker.get( pi ) ) );
            // save pivot in last index
            tracker.swap( pi, rightIndex - 1, 1 );
            long left = clearCollision( dataCache.get( tracker.get( li ) ) );
            long right = clearCollision( dataCache.get( tracker.get( ri ) ) );
            while ( li < ri )
            {
                if ( comparator.lt( left, pivot ) )
                {   // this value is on the correct side of the pivot, moving on
                    left = clearCollision( dataCache.get( tracker.get( ++li ) ) );
                }
                else if ( comparator.ge( right, pivot ) )
                {   // this value is on the correct side of the pivot, moving on
                    right = clearCollision( dataCache.get( tracker.get( --ri ) ) );
                }
                else
                {   // this value is on the wrong side of the pivot, swapping
                    tracker.swap( li, ri, 1 );
                    long temp = left;
                    left = right;
                    right = temp;
                }
            }
            int partingIndex = ri;
            if ( comparator.lt( right, pivot ) )
            {
                partingIndex++;
            }
            // restore pivot
            tracker.swap( rightIndex - 1, partingIndex, 1 );
            return partingIndex;
        }

        private void qsort( int initialStart, int initialEnd )
        {
            PrimitiveIntStack stack = new PrimitiveIntStack( 100 );
            stack.push( initialStart );
            stack.push( initialEnd );
            while ( !stack.isEmpty() )
            {
                int end = stack.poll();
                int start = stack.poll();
                int diff = end - start;
                if ( diff < 2 )
                {
                    incrementProgress( 2 );
                    continue;
                }

                incrementProgress( 1 );

                // choose a random pivot between start and end
                int pivot = start + random.nextInt( diff );
                pivot = informedPivot( start, end, pivot );

                // partition, given that pivot
                pivot = partition( start, end, pivot );
                if ( pivot > start )
                {   // there are elements to left of pivot
                    stack.push( start );
                    stack.push( pivot );
                }
                if ( pivot + 1 < end )
                {   // there are elements to right of pivot
                    stack.push( pivot + 1 );
                    stack.push( end );
                }
            }
        }

        private int informedPivot( int start, int end, int randomIndex )
        {
            if ( end-start < pivotChoice.length )
            {
                return randomIndex;
            }

            int low = Math.max( start, randomIndex - 5 );
            int high = Utils.safeCastLongToInt( Math.min( low + 10, end ) );
            int length = high-low;

            for ( int i = low, j = 0; i < high; i++,j++ )
            {
                pivotChoice[j] = clearCollision( dataCache.get( tracker.get( i ) ) );
            }
            Arrays.sort( pivotChoice, 0, length );

            long middle = pivotChoice[length/2];
            for ( int i = low; i <= high; i++ )
            {
                if ( clearCollision( dataCache.get( tracker.get( i ) ) ) == middle )
                {
                    return i;
                }
            }
            throw new ThisShouldNotHappenError( "Mattias and Raghu",
                    "The middle value somehow dissappeared in front of our eyes" );
        }
    }

    /**
     * Sets the initial tracker indexes pointing to data indexes. Only touches a designated part of trackerCache
     * so that many can run in parallel on their own part without synchronization.
     */
    private class TrackerInitializer implements Runnable
    {
        private final int[] rangeParams;
        private final int lowRadixRange;
        private final int highRadixRange;
        private final int threadIndex;
        private int bucketIndex;
        private final long[] result;

        TrackerInitializer( int threadIndex, int[] rangeParams, int lowRadixRange, int highRadixRange, long[] result )
        {
            this.threadIndex = threadIndex;
            this.rangeParams = rangeParams;
            this.lowRadixRange = lowRadixRange;
            this.highRadixRange = highRadixRange;
            this.result = result;
        }

        @Override
        public void run()
        {
            for ( long i = 0; i <= highestSetIndex; i++ )
            {
                int rIndex = radixCalculator.radixOf( dataCache.get( i ) );
                if ( rIndex > lowRadixRange && rIndex <= highRadixRange )
                {
                    long trackerIndex = (rangeParams[0] + bucketIndex++);
                    assert tracker.get( trackerIndex ) == -1 :
                            "Overlapping buckets i:" + i + ", k:" + threadIndex + ", index:" + trackerIndex;
                    tracker.set( trackerIndex, (int) i );
                    if ( bucketIndex == rangeParams[1] )
                    {
                        result[0] = highRadixRange;
                        result[1] = rangeParams[0];
                    }
                }
            }
        }
    }
}
