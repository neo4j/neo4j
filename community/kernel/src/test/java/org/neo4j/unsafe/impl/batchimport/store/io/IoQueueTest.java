/*
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
package org.neo4j.unsafe.impl.batchimport.store.io;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.util.SimplePool;
import org.neo4j.unsafe.impl.batchimport.executor.DynamicTaskExecutor;
import org.neo4j.unsafe.impl.batchimport.executor.Task;
import org.neo4j.unsafe.impl.batchimport.executor.TaskExecutor;
import org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.Writer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.unsafe.impl.batchimport.executor.DynamicTaskExecutor.DEFAULT_PARK_STRATEGY;
import static org.neo4j.unsafe.impl.batchimport.store.BatchingPageCache.SYNCHRONOUS;

public class IoQueueTest
{
    private TaskExecutor<Void> executor;
    private IoQueue queue;
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

        executor.shutdown( true );

        // THEN
        verify( executor, times( 1 ) ).submit( any( Task.class ) );
        verify( channel ).write( buffer, 100 );
        verifyNoMoreInteractions( channel );
    }

    @SuppressWarnings("unchecked")
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

        executor.shutdown( true );

        // THEN

        // Depending on race between executor and the job offers, it should be 2-3 invocations
        verify( executor, atLeast( 2 ) ).submit( any( Task.class ) );
        verify( executor, atMost( 3 ) ).submit( any( Task.class ) );

        // THEN
        executor.shutdown( true );
        verify( channel1 ).write( buffer, position1 );
        verify( channel1 ).write( buffer, position2 );
        verify( channel2 ).write( buffer, position3 );

        verifyNoMoreInteractions( channel1 );
        verifyNoMoreInteractions( channel2 );
    }

    @Before
    public void before()
    {
        executor = spy( new DynamicTaskExecutor<Void>( 3, 3, 20, DEFAULT_PARK_STRATEGY, getClass().getSimpleName() ) );
        queue = new IoQueue( executor, SYNCHRONOUS );
    }

    @After
    public void after()
    {
        queue.shutdown();
    }
}
