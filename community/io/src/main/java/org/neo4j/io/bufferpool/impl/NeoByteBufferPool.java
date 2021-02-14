/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.io.bufferpool.impl;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

import org.neo4j.io.bufferpool.ByteBufferManger;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;

/**
 * Design notes:
 * <p>
 * Buffers of the same capacity are stored in buckets.
 * Each bucket can be divided into slices in order to reduce contention.
 * This is useful on large machines with a lot of CPUs.
 * If a bucket is split into slices a thread requesting a buffer simply selects a slice randomly.
 * In this regard, it is very similar to how for instance {@link LongAdder} is implemented.
 * Number of slices for each bucket can be either configured using an absolute number or with a coefficient.
 * The coefficient will be used in the following equation:
 * {@code <slice count> = ceil(<coefficient> * <number of CPUs>)}.
 * The following points should be considered when configuring the number of slices:
 * <ul>
 *     <li>Buffers are stored in {@link ConcurrentLinkedDeque} in a slice, which is a very scalable data structure</li>
 *     <li>This buffer manager is used mainly for network stack buffers and a typical workload of the database
 *     means that network buffers are allocated by far nowhere near tight-loop-like rate</li>
 * </ul>
 * The default coefficient for commonly used buffer sizes is {@code 0.125} which means creating
 * a slice for every 8 CPUs (Hyper threads are seen as CPUs by the JVM) which should be more than enough
 * even for workloads that spend unusually large proportion of time in the networking stack.
 */
public class NeoByteBufferPool extends LifecycleAdapter implements ByteBufferManger
{
    private static final Duration DEFAULT_COLLECTION_INTERVAL = Duration.ofSeconds( 20 );

    // This magical constant is used for two things.
    // Firstly, since most of the overhead of allocating
    // direct buffers comes from the need to zero them,
    // it gets more expensive the larger the buffer is.
    // Allocating very small buffers is as performant as getting them from
    // this pool, so it is just better to allocate them.
    // The number is a result of benchmarking and it is really confirmed
    // that allocating buffers up to this size (and slightly over) is super cheap.
    // Secondly, nothing can beat JVM terms of performance and efficiency
    // when working with small heap buffers, so if not insisting on a direct buffer,
    // it is better to use heap for small buffers.
    // So this number is also used as a hint for decision
    // when to use heap and when direct buffers.
    // This second use of the constant is pure intuition
    // and is not supported by any data (benchmarks).
    private static final int DEFAULT_TINY_BUFFER_THRESHOLD = 48;

    private final JobScheduler jobScheduler;
    private final Duration collectionInterval;
    private final Bucket[] buckets;
    private final MemoryTracker memoryTracker;
    private final int maxPooledBufferCapacity;
    private final int tinyBufferThreshold;

    public NeoByteBufferPool( NeoBufferPoolConfigOverride configOverride, MemoryTracker memoryTracker, JobScheduler jobScheduler )
    {
        this.jobScheduler = jobScheduler;
        if ( configOverride.getCollectionInterval() == null )
        {
            collectionInterval = DEFAULT_COLLECTION_INTERVAL;
        }
        else
        {
            collectionInterval = configOverride.getCollectionInterval();
        }

        if ( configOverride.getTinyBufferThreshold() == null )
        {
            tinyBufferThreshold = DEFAULT_TINY_BUFFER_THRESHOLD;
        }
        else
        {
            tinyBufferThreshold = configOverride.getTinyBufferThreshold();
        }
        var bucketBootstrapper = new BucketBootstrapper( configOverride, memoryTracker );
        buckets = bucketBootstrapper.getBuckets().toArray( new Bucket[0] );
        maxPooledBufferCapacity = bucketBootstrapper.getMaxPooledBufferCapacity();
        this.memoryTracker = memoryTracker;
    }

    @Override
    public void start() throws Exception
    {
        jobScheduler.scheduleRecurring(
                Group.BUFFER_POOL_MAINTENANCE,
                JobMonitoringParams.systemJob( "Buffer pool maintenance" ),
                () -> Arrays.stream( buckets ).forEach( Bucket::prunePooledBuffers ),
                collectionInterval.toSeconds(),
                TimeUnit.SECONDS );
    }

    @Override
    public void stop() throws Exception
    {
        Arrays.stream( buckets ).forEach( Bucket::releasePooledBuffers );
    }

    @Override
    public ByteBuffer acquire( int size )
    {
        if ( size < tinyBufferThreshold || size > maxPooledBufferCapacity )
        {
            return ByteBuffers.allocateDirect( size, memoryTracker );
        }

        var bucket = getBucketFor( size );
        var buffer = bucket.acquire();
        return buffer.clear().limit( size );
    }

    @Override
    public void release( ByteBuffer buffer )
    {
        if ( !buffer.isDirect() )
        {
            throw alienBufferException( buffer );
        }

        if ( buffer.capacity() < tinyBufferThreshold || buffer.capacity() > maxPooledBufferCapacity )
        {
            ByteBuffers.releaseBuffer( buffer, memoryTracker );
            return;
        }

        var bucket = getBucketFor( buffer.capacity() );
        if ( buffer.capacity() != bucket.getBufferCapacity() )
        {
            throw alienBufferException( buffer );
        }

        bucket.release( buffer );
    }

    @Override
    public int recommendNewCapacity( int minNewCapacity, int maxCapacity )
    {
        if ( minNewCapacity > maxPooledBufferCapacity )
        {
            return -1;
        }

        return Math.min( getBucketFor( minNewCapacity ).getBufferCapacity(), maxCapacity );
    }

    @Override
    public int getHeapBufferPreferenceThreshold()
    {
        return tinyBufferThreshold;
    }

    private Bucket getBucketFor( int size )
    {
        for ( int i = 0; i < buckets.length; i++ )
        {
            Bucket pool = buckets[i];
            if ( pool.getBufferCapacity() >= size )
            {
                return pool;
            }
        }

        // we should not get here, because acquire(), release() and recommendNewCapacity() checks maxPooledBufferCapacity
        throw new IllegalStateException( "There is no bucket big enough to allocate " + size + " bytes" );
    }

    private RuntimeException alienBufferException( ByteBuffer buffer )
    {
        return new IllegalArgumentException( "Trying to release a buffer not acquired from this buffer manager: " + buffer );
    }
}
