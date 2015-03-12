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
    private final IntArray tracker;
    private final int threads;
    private long[][] sortBuckets;
    private final ProgressListener progress;

    public ParallelSort( Radix radix, LongArray dataCache, IntArray tracker, int threads, ProgressListener progress )
    {
        this.progress = progress;
        this.radixIndexCount = radix.getRadixIndexCounts();
        this.radixCalculator = radix.calculator();
        this.dataCache = dataCache;
        this.tracker = tracker;
        this.threads = threads;
    }

    public long[][] run()
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
        CountDownLatch doneSignal = new CountDownLatch( threadsNeeded );
        SortWorker[] sortWorker = new SortWorker[threadsNeeded];
        progress.started( "SORT" );
        for ( int i = 0; i < threadsNeeded; i++ )
        {
            if ( sortParams[i][1] == 0 )
            {
                break;
            }
            sortWorker[i] = new SortWorker( i, sortParams[i][0], sortParams[i][1], waitSignal, doneSignal );
            sortWorker[i].start();
        }
        waitSignal.countDown();
        try
        {
            doneSignal.await();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        finally
        {
            progress.done();
        }
        return sortBuckets;
    }

    private int[][] sortRadix()
    {
        int[][] rangeParams = new int[threads][2];
        int[] bucketRange = new int[threads];
        sortBuckets = new long[threads][2];
        int bucketSize = (int) (dataCache.size() / threads);
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
                rangeParams[threadIndex][1] = (int) dataCache.size() - fullCount;
                break;
            }
        }
        progress.done();
        int[] bucketIndex = new int[threads];
        for ( int i = 0; i < threads; i++ )
        {
            bucketIndex[i] = 0;
        }
        for ( long i = 0; i < dataCache.size(); i++ )
        {
            int rIndex = radixCalculator.radixOf( dataCache.get( i ) );
            for ( int k = 0; k < threads; k++ )
            {
                //if ( rangeParams[k][0] >= rIndex )
                if ( rIndex <= bucketRange[k] )
                {
                    long temp = (rangeParams[k][0] + bucketIndex[k]++);
                    assert tracker.get( temp ) == -1 : "Overlapping buckets i:" + i + ", k:" + k + "\n" +
                            dumpBuckets( rangeParams, bucketRange, bucketIndex );
                    tracker.set( temp, (int) i );
                    if ( bucketIndex[k] == rangeParams[k][1] )
                    {
                        sortBuckets[k][0] = bucketRange[k];
                        sortBuckets[k][1] = rangeParams[k][0];
                    }
                    break;
                }
            }
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
        long left = 0, right = 0;
        while ( li < ri )
        {
            left = clearCollision( dataCache.get( tracker.get( li ) ) );
            right = clearCollision( dataCache.get( tracker.get( ri ) ) );
            if ( Utils.unsignedCompare( left, pivot, CompareType.LT ) )
            {
                //increment left to find the greater element than the pivot
                li++;
            }
            else if ( Utils.unsignedCompare( right, pivot, CompareType.GE ) )
            {
                //decrement right to find the smaller element than the pivot
                ri--;
            }
            else
            {
                //if right index is greater then only swap
                tracker.swap( li, ri, 1 );
            }
        }
        int partingIndex = ri;
        right = clearCollision( dataCache.get( tracker.get( ri ) ) );
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

    private class SortWorker extends Thread
    {
        private final int start, size;
        private final CountDownLatch doneSignal, waitSignal;
        private int workerId = -1;
        private int threadLocalProgress;

        SortWorker( int workerId, int startRange, int size, CountDownLatch wait, CountDownLatch done )
        {
            this.start = startRange;
            this.size = size;
            this.doneSignal = done;
            this.waitSignal = wait;
            this.workerId = workerId;
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
            this.setName( "SortWorker-" + workerId );
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
            doneSignal.countDown();
        }
    }
}
