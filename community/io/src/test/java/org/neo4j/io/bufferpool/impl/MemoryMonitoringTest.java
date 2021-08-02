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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.io.ByteUnit;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPool;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class MemoryMonitoringTest
{
    private final MemoryPools memoryPools = new MemoryPools();
    private final JobScheduler jobScheduler = mock( JobScheduler.class );
    private final ArgumentCaptor<Runnable> collectionRunnableCaptor = ArgumentCaptor.forClass( Runnable.class );
    private NeoByteBufferPool bufferPool;
    private MemoryPool memoryPool;

    @BeforeEach
    void setUp()
    {
        bufferPool = new NeoByteBufferPool( memoryPools, jobScheduler );
        memoryPool = memoryPools.getPools().stream()
                                .filter( pool -> pool.group() == MemoryGroup.CENTRAL_BYTE_BUFFER_MANAGER )
                                .findFirst()
                                .get();
    }

    @Test
    void testNativeMemoryMonitoring() throws Exception
    {
        bufferPool.start();
        verify( jobScheduler ).scheduleRecurring( eq( Group.BUFFER_POOL_MAINTENANCE ), any(), collectionRunnableCaptor.capture(), eq( 20L ),
                eq( TimeUnit.SECONDS ) );
        Runnable collectionRunnable = collectionRunnableCaptor.getValue();

        var b1 = bufferPool.acquire( 1000 );
        var b2 = bufferPool.acquire( 1000 );
        var b3 = bufferPool.acquire( 1000 );
        var b4 = bufferPool.acquire( 2000 );
        var b5 = bufferPool.acquire( 4000 );

        verifyMemory( ByteUnit.kibiBytes( 9 ), 0 );

        bufferPool.release( b2 );
        bufferPool.release( b3 );
        bufferPool.release( b5 );

        collectionRunnable.run();
        collectionRunnable.run();

        verifyMemory( ByteUnit.kibiBytes( 3 ), 0 );

        var b6 = bufferPool.acquire( 1000 );

        verifyMemory( ByteUnit.kibiBytes( 4 ), 0 );

        // we are intentionally leaking b1,
        bufferPool.release( b4 );
        bufferPool.release( b6 );

        bufferPool.stop();

        verifyMemory( ByteUnit.kibiBytes( 1 ), 0 );
    }

    @Test
    void testHeapMemoryMonitoring() throws Exception
    {
        bufferPool.start();
        bufferPool.getHeapBufferMemoryTracker().allocateHeap( ByteUnit.kibiBytes( 1 ) );
        bufferPool.getHeapBufferMemoryTracker().allocateHeap( ByteUnit.kibiBytes( 1 ) );
        bufferPool.getHeapBufferMemoryTracker().allocateHeap( ByteUnit.kibiBytes( 1 ) );
        bufferPool.getHeapBufferMemoryTracker().allocateHeap( ByteUnit.kibiBytes( 2 ) );
        bufferPool.getHeapBufferMemoryTracker().allocateHeap( ByteUnit.kibiBytes( 4 ) );

        verifyMemory( 0, ByteUnit.kibiBytes( 9 ) );

        bufferPool.getHeapBufferMemoryTracker().releaseHeap( ByteUnit.kibiBytes( 1 ) );
        bufferPool.getHeapBufferMemoryTracker().releaseHeap( ByteUnit.kibiBytes( 1 ) );
        bufferPool.getHeapBufferMemoryTracker().releaseHeap( ByteUnit.kibiBytes( 4 ) );

        verifyMemory( 0, ByteUnit.kibiBytes( 3 ) );

        bufferPool.getHeapBufferMemoryTracker().allocateHeap( ByteUnit.kibiBytes( 1 ) );

        verifyMemory( 0, ByteUnit.kibiBytes( 4 ) );

        // we are intentionally leaking some memory,
        bufferPool.getHeapBufferMemoryTracker().releaseHeap( ByteUnit.kibiBytes( 1 ) );
        bufferPool.getHeapBufferMemoryTracker().releaseHeap( ByteUnit.kibiBytes( 2 ) );

        bufferPool.stop();

        verifyMemory( 0, ByteUnit.kibiBytes( 1 ) );
    }

    private void verifyMemory( long expectedNative, long expectedHeap )
    {
        assertEquals( expectedNative, memoryPool.usedNative() );
        assertEquals( expectedHeap, memoryPool.usedHeap() );
    }
}
