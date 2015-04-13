/**
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.helpers.progress.ProgressListener;
import org.neo4j.unsafe.impl.batchimport.Utils;
import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;

import static java.lang.Math.max;

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
    private final NumberArrayStats dataStats;
    private final IntArray tracker;
    private final NumberArrayStats trackerStats;
    private final int threads;
    private long[][] sortBuckets;
    private final ProgressListener progress;

    public ParallelSort( Radix radix, LongArray dataCache, NumberArrayStats dataStats,
            IntArray tracker, NumberArrayStats trackerStats, int threads, ProgressListener progress )
    {
        this.progress = progress;
        this.radixIndexCount = radix.getRadixIndexCounts();
        this.radixCalculator = radix.calculator();
        this.dataCache = dataCache;
        this.dataStats = dataStats;
        this.tracker = tracker;
        this.trackerStats = trackerStats;
        this.threads = threads;
    }

    public long[][] run() throws InterruptedException
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
        CountDownLatch waitSignal = new CountDownLatch( 1 );
        Workers<SortWorker> sortWorkers = new Workers<>( "SortWorker" );
        progress.started( "SORT" );
        for ( int i = 0; i < threadsNeeded; i++ )
        {
            if ( sortParams[i][1] == 0 )
            {
                break;
            }
            sortWorkers.start( new SortWorker( sortParams[i][0], sortParams[i][1], waitSignal ) );
        }
        waitSignal.countDown();
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
        int bucketSize = safeCastLongToInt( dataStats.size() / threads );
        int count = 0, fullCount = 0 + 0;
        rangeParams[0][0] = 0;
        bucketRange[0] = 0;
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
                rangeParams[threadIndex][1] = safeCastLongToInt( dataStats.size() - fullCount );
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
        long highestIndex = -1, size = 0;
        int i = 0;
        for ( TrackerInitializer initializer : initializers )
        {
            bucketIndex[i++] = initializer.bucketIndex;
            highestIndex = max( highestIndex, initializer.highestIndex );
            size += initializer.size;
        }
        trackerStats.set( size, highestIndex );
        if ( error != null )
        {
            throw new AssertionError( error.getMessage() + "\n" + dumpBuckets( rangeParams, bucketRange, bucketIndex ) );
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

    private int partition( int leftIndex, int rightIndex, int pivotIndex )
    {
        int li = leftIndex, ri = rightIndex - 2, pi = pivotIndex;
        long pivot = clearCollision( dataCache.get( tracker.get( pi ) ) );
        //save pivot in last index
        tracker.swap( pi, rightIndex - 1, 1 );
        long left = clearCollision( dataCache.get( tracker.get( li ) ) );
        long right = clearCollision( dataCache.get( tracker.get( ri ) ) );
        while ( li < ri )
        {
            if ( Utils.unsignedCompare( left, pivot, CompareType.LT ) )
            {
                //increment left to find the greater element than the pivot
                left = clearCollision( dataCache.get( tracker.get( ++li ) ) );
            }
            else if ( Utils.unsignedCompare( right, pivot, CompareType.GE ) )
            {
                //decrement right to find the smaller element than the pivot
                right = clearCollision( dataCache.get( tracker.get( --ri ) ) );
            }
            else
            {
                //if right index is greater then only swap
                tracker.swap( li, ri, 1 );
                long temp = left;
                left = right;
                right = temp;
            }
        }
        int partingIndex = ri;
        if ( Utils.unsignedCompare( right, pivot, CompareType.LT ) )
        {
            partingIndex++;
        }
        //restore pivot
        tracker.swap( rightIndex - 1, partingIndex, 1 );
        return partingIndex;
    }

    private void recursiveQsort( int start, int end, Random random, SortWorker workerProgress )
    {
        int diff = end - start;
        if ( diff < 2 )
        {
            workerProgress.incrementProgress( diff );
            return;
        }

        workerProgress.incrementProgress( 1 );

        // choose a random pivot between start and end
        int pivot = start + random.nextInt( diff );

        pivot = partition( start, end, pivot );

        recursiveQsort( start, pivot, random, workerProgress );
        recursiveQsort( pivot + 1, end, random, workerProgress );
    }

    /**
     * Sorts a part of data in dataCache covered by trackerCache. Values in data cache doesn't change location,
     * instead trackerCache is updated to point to the right indexes. Only touches a designated part of trackerCache
     * so that many can run in parallel on their own part without synchronization.
     */
    private class SortWorker implements Runnable
    {
        private final int start, size;
        private final CountDownLatch waitSignal;
        private int threadLocalProgress;

        SortWorker( int startRange, int size, CountDownLatch wait )
        {
            this.start = startRange;
            this.size = size;
            this.waitSignal = wait;
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
            Random random = ThreadLocalRandom.current();
            try
            {
                waitSignal.await();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
            recursiveQsort( start, start + size, random, this );
            reportProgress();
        }
    }

    /**
     * Sets the initial tracker indexes pointing to data indexes. Only touches a designated part of trackerCache
     * so that many can run in parallel on their own part without synchronization.
     */
    private class TrackerInitializer implements Runnable
    {
        private final int[] rangeParams;
        private final int lowBucketRange;
        private final int highBucketRange;
        private final int threadIndex;
        private int bucketIndex;
        private final long[] result;
        private long highestIndex = -1;
        private long size;

        TrackerInitializer( int threadIndex, int[] rangeParams, int lowBucketRange, int highBucketRange, long[] result )
        {
            this.threadIndex = threadIndex;
            this.rangeParams = rangeParams;
            this.lowBucketRange = lowBucketRange;
            this.highBucketRange = highBucketRange;
            this.result = result;
        }

        @Override
        public void run()
        {
            long max = dataStats.highestIndex();
            for ( long i = 0; i <= max; i++ )
            {
                int rIndex = radixCalculator.radixOf( dataCache.get( i ) );
                if ( rIndex > lowBucketRange && rIndex <= highBucketRange )
                {
                    long temp = (rangeParams[0] + bucketIndex++);
                    assert tracker.get( temp ) == -1 : "Overlapping buckets i:" + i + ", k:" + threadIndex;
                    tracker.set( temp, (int) i );
                    if ( bucketIndex == rangeParams[1] )
                    {
                        result[0] = highBucketRange;
                        result[1] = rangeParams[0];
                    }
                }
            }
            if ( bucketIndex > 0 )
            {
                highestIndex = rangeParams[0] + bucketIndex - 1;
            }
            size = bucketIndex;
        }
    }
}
