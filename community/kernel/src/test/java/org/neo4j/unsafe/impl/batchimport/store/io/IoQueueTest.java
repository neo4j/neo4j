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
package org.neo4j.unsafe.impl.batchimport.store.io;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TargetDirectory.TestDirectory;
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

public class IoQueueTest
{
    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldExecuteWriteJob() throws Exception
    {
        // GIVEN
        ExecutorService executor = cleanupRule.add( spy( Executors.newFixedThreadPool( 3 ) ) );
        IoQueue queue = new IoQueue( executor );
        File file = new File( directory.directory(), "file" );
        StoreChannel channel = spy( fs.create( file ) );
        Monitor monitor = mock( Monitor.class );
        Writer writer = queue.create( file, channel, monitor );
        Ring<ByteBuffer> ring = mock( Ring.class );
        ByteBuffer buffer = ByteBuffer.allocate( 10 );
        int position = 100;

        // WHEN
        writer.write( buffer, position, ring );
        verify( executor, times( 1 ) ).submit( any( Callable.class ) );

        // THEN
        executor.shutdown();
        executor.awaitTermination( 10, SECONDS );
        verify( channel, times( 1 ) ).write( buffer, position );
        verifyNoMoreInteractions( channel );
    }

    @SuppressWarnings( "unchecked" )
    @Test
    public void shouldExecuteWriteJobsForMultipleFiles() throws Exception
    {
        // GIVEN
        ExecutorService executor = cleanupRule.add( spy( Executors.newFixedThreadPool( 3 ) ) );
        IoQueue queue = new IoQueue( executor );
        File file1 = new File( directory.directory(), "file1" );
        StoreChannel channel1 = cleanupRule.add( spy( fs.create( file1 ) ) );
        File file2 = new File( directory.directory(), "file2" );
        StoreChannel channel2 = cleanupRule.add( spy( fs.create( file2 ) ) );
        Monitor monitor = mock( Monitor.class );
        Writer writer1 = queue.create( file1, channel1, monitor );
        Writer writer2 = queue.create( file2, channel2, monitor );
        Ring<ByteBuffer> ring1 = mock( Ring.class );
        Ring<ByteBuffer> ring2 = mock( Ring.class );
        ByteBuffer buffer = ByteBuffer.allocate( 10 );
        int position1 = 100, position2 = position1+buffer.capacity(), position3 = 50;

        // WHEN
        writer1.write( buffer, position1, ring1 );
        writer1.write( buffer, position2, ring1 );
        writer2.write( buffer, position3, ring2 );
        // Depending on race between executor and the job offers, it should be 2-3 invocations
        verify( executor, atLeast( 2 ) ).submit( any( Callable.class ) );
        verify( executor, atMost( 3 ) ).submit( any( Callable.class ) );

        // THEN
        executor.shutdown();
        executor.awaitTermination( 10, SECONDS );
        verify( channel1, times( 1 ) ).write( buffer, position1 );
        verify( channel1, times( 1 ) ).write( buffer, position2 );
        verify( channel2, times( 1 ) ).write( buffer, position3 );
        verifyNoMoreInteractions( channel1 );
        verifyNoMoreInteractions( channel2 );
    }

    public final @Rule TestDirectory directory = TargetDirectory.testDirForTest( getClass() );
    public final @Rule CleanupRule cleanupRule = new CleanupRule();
    private final FileSystemAbstraction fs = new DefaultFileSystemAbstraction();
}
