/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import java.util.concurrent.CountDownLatch;

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

    public ParallelSort( Radix radix, LongArray dataCache, IntArray tracker, int threads )
    {
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
                    count = radixIndexCount[i];
                }
                else
                {
                    rangeParams[threadIndex][1] = radixIndexCount[i];
                    fullCount += radixIndexCount[i];
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
                    assert tracker.get( temp ) == -1;
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

    private int partition( int leftIndex, int rightIndex, int pivotIndex )
    {
        int li = leftIndex, ri = rightIndex - 2, pi = pivotIndex;
        long pivot = clearCollision( dataCache.get( tracker.get( pi ) ) );
        //save pivot in last index
        swapElement( tracker, pi, rightIndex - 1 );
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
                swapElement( tracker, li, ri );
            }
        }
        int partingIndex = ri;
        right = clearCollision( dataCache.get( tracker.get( ri ) ) );
        if ( Utils.unsignedCompare( right, pivot, CompareType.LT ) )
        {
            partingIndex++;
        }
        //restore pivot
        swapElement( tracker, rightIndex - 1, partingIndex );
        return partingIndex;
    }

    public void recursiveQsort( int start, int end )
    {
        if ( end - start < 2 )
        {
            return;
        }
        //choose the middle value
        int pivot = start + ((end - start) / 2);

        pivot = partition( start, end, pivot );

        recursiveQsort( start, pivot );
        recursiveQsort( pivot + 1, end );
    }

    private class SortWorker extends Thread
    {
        private final int start, size;
        private final CountDownLatch doneSignal, waitSignal;
        private int workerId = -1;

        SortWorker( int workerId, int startRange, int size, CountDownLatch wait, CountDownLatch done )
        {
            start = startRange;
            this.size = size;
            this.doneSignal = done;
            waitSignal = wait;
            this.workerId = workerId;
        }

        @Override
        public void run()
        {
            this.setName( "SortWorker-" + workerId );
            try
            {
                waitSignal.await();
            }
            catch ( InterruptedException e )
            {
                e.printStackTrace();
                //ignore
            }
            recursiveQsort( start, start + size );
            doneSignal.countDown();
        }
    }

    private static void swapElement( IntArray trackerCache, int left, int right )
    {
        trackerCache.swap( left, right, 1 );
    }
}
