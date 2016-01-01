/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.store.io;

import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.test.CleanupRule;
import org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.Writer;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.SYNCHRONOUS;

public class IoQueueTest
{
    @Rule
    public final CleanupRule cleanupRule = new CleanupRule();

    private final ExecutorService executor = cleanupRule.add( spy( Executors.newFixedThreadPool( 3 ) ) );
    private final IoQueue queue = new IoQueue( executor, SYNCHRONOUS );
    private final Monitor monitor = mock( Monitor.class );
    private final ByteBuffer buffer = ByteBuffer.allocate( 10 );

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldExecuteWriteJob() throws Exception
    {
        // GIVEN
        StoreChannel channel = mock( StoreChannel.class );
        Writer writer = queue.create( channel, monitor );
        SimplePool<ByteBuffer> pool = mock( SimplePool.class );
        int position = 100;

        // WHEN
        writer.write( buffer, position, pool );

        executor.shutdown();
        executor.awaitTermination( 10, SECONDS );

        // THEN
        verify( executor, times( 1 ) ).submit( any( Callable.class ) );

        verify( channel, times( 1 ) ).write( buffer, position );
        verifyNoMoreInteractions( channel );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldExecuteWriteJobsForMultipleFiles() throws Exception
    {
        // GIVEN
        StoreChannel channel1 = mock( StoreChannel.class );
        StoreChannel channel2 = mock( StoreChannel.class );
        Writer writer1 = queue.create( channel1, monitor );
        Writer writer2 = queue.create( channel2, monitor );
        SimplePool<ByteBuffer> pool1 = mock( SimplePool.class );
        SimplePool<ByteBuffer> pool2 = mock( SimplePool.class );

        final int position1 = 100;
        final int position2 = position1 + buffer.capacity();
        final int position3 = 50;

        // WHEN
        writer1.write( buffer, position1, pool1 );
        writer1.write( buffer, position2, pool1 );
        writer2.write( buffer, position3, pool2 );

        executor.shutdown();
        executor.awaitTermination( 10, SECONDS );

        // THEN

        // Depending on race between executor and the job offers, it should be 2-3 invocations
        verify( executor, atLeast( 2 ) ).submit( any( Callable.class ) );
        verify( executor, atMost( 3 ) ).submit( any( Callable.class ) );

        verify( channel1, times( 1 ) ).write( buffer, position1 );
        verify( channel1, times( 1 ) ).write( buffer, position2 );
        verify( channel2, times( 1 ) ).write( buffer, position3 );
        verifyNoMoreInteractions( channel1 );
        verifyNoMoreInteractions( channel2 );
    }
}
