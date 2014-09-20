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

import org.neo4j.collection.primitive.PrimitiveIntStack;
import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;

/**
 * Sorts input data by dividing up into chunks and sort each chunk in parallel. Each chunk is sorted
 * using a quick sort method, whereas the dividing of the data is first sorted using radix sort.
 */
public class ParallelSort
{
    private static final int PROGRESS_INTERVAL = 100;
    private static final int RADIX_BITS = 24;
    private static final int LENGTH_MASK = (int) (0xFE000000_00000000L >>> (64 - RADIX_BITS));
    private static final int HASHCODE_MASK = (int) (0x00FFFF00_00000000L >>> (64 - RADIX_BITS));

    private final int[] radixIndexCount;
    private final LongArray dataCache;
    private final IntArray tracker;
    private final int threads;
    private long[][] sortBuckets;
    private int iterations;

    public ParallelSort( int[] radixIndexCount, LongArray dataCache, IntArray tracker, int threads )
    {
        this.radixIndexCount = radixIndexCount;
        this.dataCache = dataCache;
        this.tracker = tracker;
        this.threads = threads;
    }

    public long[][] run()
    {
        int[][] sortParams = sortRadix( radixIndexCount, dataCache, tracker, threads );
        CountDownLatch waitSignal = new CountDownLatch( 1 );
        CountDownLatch doneSignal = new CountDownLatch( threads );
        SortWorker[] sortWorker = new SortWorker[threads];
        for ( int i = 0; i < threads; i++ )
        {
            sortWorker[i] = new SortWorker( i, sortParams[i][0], sortParams[i][1], dataCache, tracker, waitSignal,
                    doneSignal );
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
        addIterations( -1 );
        return sortBuckets;
    }

    private synchronized int addIterations( int val )
    {
        if ( val >= 0 )
        {
            iterations += val;
        }
        else
        {
            iterations = 0;
        }
        return iterations;
    }

    private int getRadix( long val )
    {
        int index = (int) (val >>> (64 - RADIX_BITS));
        index = (((index & LENGTH_MASK) >>> 1) | (index & HASHCODE_MASK));
        return index;
    }

    private int[][] sortRadix( int[] radixIndexCount, LongArray dataCache, IntArray tracker, int threads )
    {
        int[][] rangeParams = new int[threads][2];
        int[] bucketRange = new int[threads];
        sortBuckets = new long[threads][2];
        long dataCacheSize = dataCache.highestSetIndex()+1;
        int bucketSize = (int) (dataCacheSize / threads);
        int count = 0, fullCount = 0 + 0;
        rangeParams[0][0] = 0;
        bucketRange[0] = 0;
        for ( int i = 0, threadIndex = 0; i < radixIndexCount.length && threadIndex < threads; i++ )
        {
            if ( (count + radixIndexCount[i]) > bucketSize )
            {
                bucketRange[threadIndex] = i - 1;
                rangeParams[threadIndex + 1][0] = fullCount;
                rangeParams[threadIndex][1] = count;
                count = 0;
                threadIndex++;
            }
            if ( threadIndex == threads - 1 )
            {
                bucketRange[threadIndex] = radixIndexCount.length;
                rangeParams[threadIndex][1] = (int) dataCacheSize - fullCount;
                break;
            }
            else
            {
                count += radixIndexCount[i];
                fullCount += radixIndexCount[i];
            }
        }
        int[] bucketIndex = new int[threads];
        for ( int i = 0; i < threads; i++ )
        {
            bucketIndex[i] = 0;
        }
        for ( long i = 0; i < dataCacheSize; i++ )
        {
            int rIndex = getRadix( dataCache.get( i ) );
            for ( int k = 0; k < threads; k++ )
            {
                //if ( rangeParams[k][0] >= rIndex )
                if ( rIndex <= bucketRange[k] )
                {
                    long temp = (rangeParams[k][0] + bucketIndex[k]++);
                    long temp1 = tracker.get( temp );
                    if ( temp1 != -1 )
                    {
                        System.out.println( "error in init of tracker" );
                    }
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

    private void qSort( int workerId, int start, int size, LongArray dataCache, IntArray tracker )
    {
        PrimitiveIntStack stack = new PrimitiveIntStack( 100 );
        int pivotIndex = start;//0;
        int leftIndex = pivotIndex + 1;
        int rightIndex = start + size - 1;//trackerCache.size() - 1;
        stack.push( pivotIndex );//push always with left and right
        stack.push( rightIndex );
        int leftIndexOfSubSet, rightIndexOfSubset;
        long iteration = 0, swaps = 0, compares = 0;
        int[] vals = new int[size];
        for ( int i = 0; i < size; i++ )
        {
            int index = tracker.get( start + i );
            if ( index != -1 )
            {
                vals[i] = getRadix( dataCache.get( index ) );
            }
            else
            {
                vals[i] = -1;
            }
        }
        while ( !stack.isEmpty() )
        {
            //pop always with right and left
            rightIndexOfSubset = stack.poll();
            leftIndexOfSubSet = stack.poll();
            leftIndex = leftIndexOfSubSet + 1;
            pivotIndex = leftIndexOfSubSet;
            rightIndex = rightIndexOfSubset;
            if ( leftIndex > rightIndex )
            {
                continue;
            }
            while ( leftIndex < rightIndex )
            {
                //increment left to find the greater element than the pivot
                compares++;
                while ( (leftIndex <= rightIndex)
                        && StringIdMapper.compareDataCache( dataCache, tracker, leftIndex, pivotIndex,
                                CompareType.LE ) )
                {
                    compares++;
                    leftIndex++;
                }
                //decrement right to find the smaller element than the pivot
                compares++;
                while ( (leftIndex <= rightIndex)
                        && StringIdMapper.compareDataCache( dataCache, tracker, rightIndex, pivotIndex,
                                CompareType.GE ) )
                {
                    compares++;
                    rightIndex--;
                }
                //if right index is greater then only swap
                if ( rightIndex >= leftIndex )
                {
                    swaps++;
                    swapElement( tracker, leftIndex, rightIndex );
                }
            }
            if ( pivotIndex <= rightIndex )
            {
                compares++;
                if ( pivotIndex != rightIndex
                        && StringIdMapper.compareDataCache( dataCache, tracker, pivotIndex, rightIndex,
                                CompareType.GT ) )
                {
                    swaps++;
                    swapElement( tracker, pivotIndex, rightIndex );
                }
            }
            if ( leftIndexOfSubSet < rightIndex )
            {
                stack.push( leftIndexOfSubSet );
                stack.push( rightIndex - 1 );
            }
            if ( rightIndexOfSubset > rightIndex )
            {
                stack.push( rightIndex + 1 );
                stack.push( rightIndexOfSubset );
            }
            if ( iteration++ % PROGRESS_INTERVAL == 0 )
            {
                addIterations( PROGRESS_INTERVAL );
            }
        }
    }

    private class SortWorker extends Thread
    {
        private final int start, size;
        private final CountDownLatch doneSignal, waitSignal;
        private final LongArray dataCache;
        private final IntArray tracker;
        private int workerId = -1;

        SortWorker( int workerId, int startRange, int size, LongArray dataCache, IntArray tracker, CountDownLatch wait,
                CountDownLatch done )
        {
            start = startRange;
            this.size = size;
            this.doneSignal = done;
            this.dataCache = dataCache;
            this.tracker = tracker;
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
            qSort( workerId, start, size, dataCache, tracker );
            doneSignal.countDown();
        }
    }

    private static void swapElement( IntArray trackerCache, int left, int right )
    {
        int temp = trackerCache.get( left );
        trackerCache.set( left, trackerCache.get( right ) );
        trackerCache.set( right, temp );
    }
}
