/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.memory;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.neo4j.io.memory.ByteBufferFactory.HEAP_ALLOCATOR;
import static org.neo4j.io.memory.ByteBuffers.allocate;

class ByteBufferFactoryTest
{
    @Test
    void shouldCloseGlobalAllocationsOnClose()
    {
        // given
        ByteBufferFactory.Allocator allocator = mock( ByteBufferFactory.Allocator.class );
        when( allocator.allocate( anyInt() ) ).thenAnswer( invocationOnMock -> allocate( invocationOnMock.getArgument( 0 ) ) );
        ByteBufferFactory factory = new ByteBufferFactory( () -> allocator, 100 );

        // when doing some allocations that are counted as global
        factory.acquireThreadLocalBuffer();
        factory.releaseThreadLocalBuffer();
        factory.acquireThreadLocalBuffer();
        factory.releaseThreadLocalBuffer();
        factory.globalAllocator().allocate( 123 );
        factory.globalAllocator().allocate( 456 );
        // and closing it
        factory.close();

        // then
        InOrder inOrder = inOrder( allocator );
        inOrder.verify( allocator, times( 1 ) ).allocate( 100 );
        inOrder.verify( allocator, times( 1 ) ).allocate( 123 );
        inOrder.verify( allocator, times( 1 ) ).allocate( 456 );
        inOrder.verify( allocator, times( 1 ) ).close();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    void shouldCreateNewInstancesOfLocalAllocators()
    {
        // given
        Supplier<ByteBufferFactory.Allocator> allocator = mock( Supplier.class );
        when( allocator.get() ).thenAnswer( invocationOnMock -> mock( ByteBufferFactory.Allocator.class ) );
        ByteBufferFactory factory = new ByteBufferFactory( allocator, 100 );

        // when
        ByteBufferFactory.Allocator localAllocator1 = factory.newLocalAllocator();
        ByteBufferFactory.Allocator localAllocator2 = factory.newLocalAllocator();
        localAllocator2.close();
        ByteBufferFactory.Allocator localAllocator3 = factory.newLocalAllocator();

        // then
        assertNotSame( localAllocator1, localAllocator2 );
        assertNotSame( localAllocator2, localAllocator3 );
        assertNotSame( localAllocator1, localAllocator3 );
    }

    @Test
    void shouldFailAcquireThreadLocalBufferIfAlreadyAcquired()
    {
        // given
        ByteBufferFactory factory = new ByteBufferFactory( () -> HEAP_ALLOCATOR, 1024 );
        factory.acquireThreadLocalBuffer();

        // when/then
        assertThrows( IllegalStateException.class, factory::acquireThreadLocalBuffer );
        factory.close();
    }

    @Test
    void shouldFailReleaseThreadLocalBufferIfNotAcquired()
    {
        // given
        ByteBufferFactory factory = new ByteBufferFactory( () -> HEAP_ALLOCATOR, 1024 );
        factory.acquireThreadLocalBuffer();
        factory.releaseThreadLocalBuffer();

        // when/then
        assertThrows( IllegalStateException.class, factory::releaseThreadLocalBuffer );
        factory.close();
    }

    @Test
    void shouldShareThreadLocalBuffersStressfully() throws Throwable
    {
        // given
        ByteBufferFactory factory = new ByteBufferFactory( () -> HEAP_ALLOCATOR, 1024 );
        int threads = 10;
        CountDownLatch startLatch = new CountDownLatch( 1 );
        ExecutorService executor = Executors.newFixedThreadPool( threads );
        List<Future<?>> futures = new ArrayList<>();
        List<Set<ByteBuffer>> seenBuffers = new ArrayList<>();
        for ( int i = 0; i < threads; i++ )
        {
            HashSet<ByteBuffer> seen = new HashSet<>();
            seenBuffers.add( seen );
            futures.add( executor.submit( () ->
            {
                startLatch.await();
                for ( int j = 0; j < 1000; j++ )
                {
                    ByteBuffer buffer = factory.acquireThreadLocalBuffer();
                    assertNotNull( buffer );
                    seen.add( buffer );
                    factory.releaseThreadLocalBuffer();
                }
                return null;
            } ) );
        }

        // when
        startLatch.countDown();
        for ( Future<?> future : futures )
        {
            future.get();
        }
        executor.shutdown();
        executor.awaitTermination( 10, TimeUnit.SECONDS );

        // then
        for ( int i = 0; i < threads; i++ )
        {
            assertEquals( 1, seenBuffers.get( i ).size() );
        }
        factory.close();
    }
}
