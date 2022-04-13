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
package org.neo4j.io.pagecache.impl.muninn.versioned;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Isolated;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.util.concurrent.Futures;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.io.pagecache.impl.muninn.versioned.InMemoryVersionStorage.MAX_VERSIONED_STORAGE;

@Isolated
class InMemoryVersionStorageTest
{

    private boolean nativeAccess;

    @BeforeEach
    void setUp()
    {
        nativeAccess = UnsafeUtil.exchangeNativeAccessCheckEnabled( false );
    }

    @AfterEach
    void tearDown()
    {
        UnsafeUtil.exchangeNativeAccessCheckEnabled( nativeAccess );
    }

    @Test
    void copyPageToNewAddress() throws Throwable
    {
        int directlyAllocatedCapacity = PageCache.PAGE_SIZE;
        var memoryTracker = new LocalMemoryTracker();
        var allocator = MemoryAllocator.createAllocator( MAX_VERSIONED_STORAGE, memoryTracker );
        long allocatedMemory = 0;
        try
        {
            allocatedMemory = UnsafeUtil.allocateMemory( directlyAllocatedCapacity, EmptyMemoryTracker.INSTANCE );
            ByteBuffer sourceBuffer = UnsafeUtil.newDirectByteBuffer( allocatedMemory, directlyAllocatedCapacity );
            sourceBuffer.putLong( 42 );

            var versionStorage = new InMemoryVersionStorage( allocator, PageCache.PAGE_SIZE );
            long destination = versionStorage.copyPage( allocatedMemory );

            assertNotEquals( allocatedMemory, destination );
            ByteBuffer destinationBuffer = UnsafeUtil.newDirectByteBuffer( destination, directlyAllocatedCapacity );
            assertEquals( 42, destinationBuffer.getLong() );
        }
        finally
        {
            UnsafeUtil.free( allocatedMemory, directlyAllocatedCapacity, EmptyMemoryTracker.INSTANCE );
        }
    }

    @Test
    void copyPageByMultipleThreads() throws Throwable
    {
        int directlyAllocatedCapacity = PageCache.PAGE_SIZE;
        var memoryTracker = new LocalMemoryTracker();
        var allocator = MemoryAllocator.createAllocator( MAX_VERSIONED_STORAGE, memoryTracker );
        long allocatedMemory = 0;
        var executors = Executors.newFixedThreadPool( 20 );
        int copyTasks = 1000;
        try
        {
            allocatedMemory = UnsafeUtil.allocateMemory( directlyAllocatedCapacity, EmptyMemoryTracker.INSTANCE );

            long sourcePage = allocatedMemory;
            var versionStorage = new InMemoryVersionStorage( allocator, PageCache.PAGE_SIZE );

            CountDownLatch startLatch = new CountDownLatch( 1 );
            var futures = new ArrayList<Future<?>>();
            for ( int i = 0; i < copyTasks; i++ )
            {
                futures.add( executors.submit( () ->
                {
                    try
                    {
                        startLatch.await();
                        versionStorage.copyPage( sourcePage );
                    }
                    catch ( InterruptedException e )
                    {
                        throw new RuntimeException( e );
                    }
                } ) );
            }
            startLatch.countDown();
            Futures.getAll( futures );

            assertEquals( copyTasks * PageCache.PAGE_SIZE, versionStorage.getAllocatedBytes() );
        }
        finally
        {
            UnsafeUtil.free( allocatedMemory, directlyAllocatedCapacity, EmptyMemoryTracker.INSTANCE );
            executors.shutdown();
        }

    }

    @Test
    void failWithOutOfMemoryOnLimit()
    {
        int direcltyAllocatedCapacity = Long.BYTES;
        var memoryTracker = new LocalMemoryTracker();
        var allocator = MemoryAllocator.createAllocator( MAX_VERSIONED_STORAGE, memoryTracker );
        long allocatedMemory = 0;
        try
        {
            allocatedMemory = UnsafeUtil.allocateMemory( direcltyAllocatedCapacity, EmptyMemoryTracker.INSTANCE );

            var versionStorage = new InMemoryVersionStorage( allocator, PageCache.PAGE_SIZE );
            long sourcePage = allocatedMemory;
            assertThrows( OutOfMemoryError.class, () -> {
                while ( true )
                {
                    versionStorage.copyPage( sourcePage );
                }
            } );
        }
        finally
        {
            UnsafeUtil.free( allocatedMemory, direcltyAllocatedCapacity, EmptyMemoryTracker.INSTANCE );
        }
    }
}
