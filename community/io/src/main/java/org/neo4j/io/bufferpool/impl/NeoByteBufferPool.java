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
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.util.VisibleForTesting;

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

    private final JobScheduler jobScheduler;
    private final Duration collectionInterval;
    private final Bucket[] buckets;
    private final MemoryMonitor memoryMonitor;
    private final int maxPooledBufferCapacity;

    private JobHandle<?> collectionJob;

    public NeoByteBufferPool( NeoBufferPoolConfigOverride configOverride, MemoryPools memoryPools, JobScheduler jobScheduler )
    {
        this.jobScheduler = jobScheduler;
        this.memoryMonitor = crateMemoryMonitor( memoryPools );
        if ( configOverride.getCollectionInterval() == null )
        {
            collectionInterval = DEFAULT_COLLECTION_INTERVAL;
        }
        else
        {
            collectionInterval = configOverride.getCollectionInterval();
        }

        var bucketBootstrapper = new BucketBootstrapper( configOverride, memoryMonitor.getMemoryTracker() );
        buckets = bucketBootstrapper.getBuckets().toArray( new Bucket[0] );
        maxPooledBufferCapacity = bucketBootstrapper.getMaxPooledBufferCapacity();
    }

    /**
     * The monitoring framework is a great tool for verifying that
     * the pool is doing what it is supposed to.
     */
    @VisibleForTesting
    MemoryMonitor crateMemoryMonitor( MemoryPools memoryPools )
    {
        return new MemoryMonitor( memoryPools );
    }

    @Override
    public void start() throws Exception
    {
        collectionJob = jobScheduler.scheduleRecurring(
                Group.BUFFER_POOL_MAINTENANCE,
                JobMonitoringParams.systemJob( "Buffer pool maintenance" ),
                () -> Arrays.stream( buckets ).forEach( Bucket::prunePooledBuffers ),
                collectionInterval.toSeconds(),
                TimeUnit.SECONDS );
    }

    @Override
    public void stop() throws Exception
    {
        // the collection job can return back buffers it examines,
        // so it needs to be cancelled first if we want to be sure
        // that all buffers are released when this is done.
        if ( collectionJob != null )
        {
            collectionJob.cancel();
            try
            {
                collectionJob.waitTermination();
            }
            catch ( Exception e )
            {
                // ignore
            }
        }
        Arrays.stream( buckets ).forEach( Bucket::releasePooledBuffers );
    }

    @Override
    public ByteBuffer acquire( int size )
    {
        if ( size > maxPooledBufferCapacity )
        {
            return ByteBuffers.allocateDirect( size, memoryMonitor.getMemoryTracker() );
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

        if ( buffer.capacity() > maxPooledBufferCapacity )
        {
            ByteBuffers.releaseBuffer( buffer, memoryMonitor.getMemoryTracker() );
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
            return NO_CAPACITY_PREFERENCE;
        }

        return Math.min( getBucketFor( minNewCapacity ).getBufferCapacity(), maxCapacity );
    }

    @Override
    public MemoryTracker getHeapBufferMemoryTracker()
    {
        return memoryMonitor.getMemoryTracker();
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

    private static RuntimeException alienBufferException( ByteBuffer buffer )
    {
        return new IllegalArgumentException( "Trying to release a buffer not acquired from this buffer manager: " + buffer );
    }
}
