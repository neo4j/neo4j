/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.collection.primitive.PrimitiveLongStack;
import org.neo4j.helpers.ThisShouldNotHappenError;
import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.Utils;
import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;

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
    private final Tracker tracker;
    private final int threads;
    private long[][] sortBuckets;
    private final ProgressListener progress;
    private final Comparator comparator;

    public ParallelSort( Radix radix, LongArray dataCache, long highestSetIndex,
            Tracker tracker, int threads, ProgressListener progress, Comparator comparator )
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
        long[][] sortParams = sortRadix();
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
            sortWorkers.awaitAndThrowOnError( RuntimeException.class );
        }
        finally
        {
            progress.done();
        }
        return sortBuckets;
    }

    private long[][] sortRadix() throws InterruptedException
    {
        long[][] rangeParams = new long[threads][2];
        int[] bucketRange = new int[threads];
        Workers<TrackerInitializer> initializers = new Workers<>( "TrackerInitializer" );
        sortBuckets = new long[threads][2];
        long dataSize = highestSetIndex+1;
        long bucketSize = dataSize / threads;
        long count = 0, fullCount = 0;
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
                rangeParams[threadIndex][1] = dataSize - fullCount;
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
        long[] bucketIndex = new long[threads];
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

    private String dumpBuckets( long[][] rangeParams, int[] bucketRange, long[] bucketIndex )
    {
        StringBuilder builder = new StringBuilder();
        builder.append( "rangeParams:\n" );
        for ( long[] range : rangeParams )
        {
            builder.append( "  " ).append( Arrays.toString( range ) ).append( "\n" );
        }
        builder.append( "bucketRange:\n" );
        for ( int range : bucketRange )
        {
            builder.append( "  " ).append( range ).append( "\n" );
        }
        builder.append( "bucketIndex:\n" );
        for ( long index : bucketIndex )
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

        /**
         * @param dataValue the data value in the used dataCache for a given tracker index.
         * @return actual data value given the data value retrieved from the dataCache at a given index.
         * This is exposed to be able to introduce an indirection while preparing the tracker indexes
         * just like the other methods on this interface does.
         */
        long dataValue( long dataValue );
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

        @Override
        public long dataValue( long dataValue )
        {
            return dataValue;
        }
    };

    /**
     * Sorts a part of data in dataCache covered by trackerCache. Values in data cache doesn't change location,
     * instead trackerCache is updated to point to the right indexes. Only touches a designated part of trackerCache
     * so that many can run in parallel on their own part without synchronization.
     */
    private class SortWorker implements Runnable
    {
        private final long start, size;
        private int threadLocalProgress;
        private final long[] pivotChoice = new long[10];
        private final ThreadLocalRandom random = ThreadLocalRandom.current();

        SortWorker( long startRange, long size )
        {
            this.start = startRange;
            this.size = size;
        }

        void incrementProgress( long diff )
        {
            threadLocalProgress += diff;
            if ( threadLocalProgress >= 10_000 /*reasonably big to dwarf passing a memory barrier*/ )
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

        private long partition( long leftIndex, long rightIndex, long pivotIndex )
        {
            long li = leftIndex, ri = rightIndex - 2, pi = pivotIndex;
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
            long partingIndex = ri;
            if ( comparator.lt( right, pivot ) )
            {
                partingIndex++;
            }
            // restore pivot
            tracker.swap( rightIndex - 1, partingIndex, 1 );
            return partingIndex;
        }

        private void qsort( long initialStart, long initialEnd )
        {
            PrimitiveLongStack stack = new PrimitiveLongStack( 100 );
            stack.push( initialStart );
            stack.push( initialEnd );
            while ( !stack.isEmpty() )
            {
                long end = stack.poll();
                long start = stack.poll();
                long diff = end - start;
                if ( diff < 2 )
                {
                    incrementProgress( 2 );
                    continue;
                }

                incrementProgress( 1 );

                // choose a random pivot between start and end
                long pivot = start + random.nextLong( diff );
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

        private long informedPivot( long start, long end, long randomIndex )
        {
            if ( end-start < pivotChoice.length )
            {
                return randomIndex;
            }

            long low = Math.max( start, randomIndex - 5 );
            long high = Math.min( low + 10, end );
            int length = Utils.safeCastLongToInt( high-low );

            int j = 0;
            for ( long i = low; i < high; i++, j++ )
            {
                pivotChoice[j] = clearCollision( dataCache.get( tracker.get( i ) ) );
            }
            Arrays.sort( pivotChoice, 0, length );

            long middle = pivotChoice[length/2];
            for ( long i = low; i <= high; i++ )
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
        private final long[] rangeParams;
        private final int lowRadixRange;
        private final int highRadixRange;
        private final int threadIndex;
        private long bucketIndex;
        private final long[] result;

        TrackerInitializer( int threadIndex, long[] rangeParams, int lowRadixRange, int highRadixRange,
                long[] result )
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
                int rIndex = radixCalculator.radixOf( comparator.dataValue( dataCache.get( i ) ) );
                if ( rIndex > lowRadixRange && rIndex <= highRadixRange )
                {
                    long trackerIndex = (rangeParams[0] + bucketIndex++);
                    assert tracker.get( trackerIndex ) == -1 :
                            "Overlapping buckets i:" + i + ", k:" + threadIndex + ", index:" + trackerIndex;
                    tracker.set( trackerIndex, i );
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
