/**
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
package org.neo4j.kernel.impl.nioneo.store;

import java.io.File;
import java.io.RandomAccessFile;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.ResourceCollection;
import org.neo4j.test.TargetDirectory;

import static java.nio.channels.FileChannel.MapMode.READ_WRITE;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MappedPersistenceWindowTest
{
    private static final TargetDirectory target = TargetDirectory.forTest( MappedPersistenceWindowTest.class );
    @Rule
    public final TargetDirectory.TestDirectory directory = target.testDirectory();
    @Rule
    public final ResourceCollection resources = new ResourceCollection();

    @Test
    public void shouldCloseUnusedWindow() throws Exception
    {
        // given
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        StoreChannel channel = new StoreFileChannel( file.getChannel() );
        MappedPersistenceWindow window = new MappedPersistenceWindow( 0, 8, 16, channel, READ_WRITE );

        // when
        boolean wasClosed = window.writeOutAndCloseIfFree( false );
        file.close();

        // then
        assertTrue( wasClosed );
    }

    @Test
    public void shouldNotCloseMarkedWindow() throws Exception
    {
        // given
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        StoreChannel channel = new StoreFileChannel( file.getChannel() );
        MappedPersistenceWindow window = new MappedPersistenceWindow( 0, 8, 16, channel, READ_WRITE );

        window.markAsInUse();

        // when
        boolean wasClosed = window.writeOutAndCloseIfFree( false );
        file.close();

        // then
        assertFalse( wasClosed );
    }

    @Test
    public void shouldNotCloseLockedWindow() throws Exception
    {
        // given
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        StoreChannel channel = new StoreFileChannel( file.getChannel() );
        final MappedPersistenceWindow window = new MappedPersistenceWindow( 0, 8, 16, channel, READ_WRITE );

        window.markAsInUse();
        OtherThreadExecutor<Void> executor = new OtherThreadExecutor<>( "other thread", null );
        executor.execute( new WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state )
            {
                window.lock( OperationType.WRITE );
                return null;
            }
        } );

        // when
        boolean wasClosed = window.writeOutAndCloseIfFree( false );
        file.close();

        // then
        assertFalse( wasClosed );

        executor.close();
    }

    @Test
    public void shouldCloseReleasedWindow() throws Exception
    {
        // given
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        StoreChannel channel = new StoreFileChannel( file.getChannel() );
        MappedPersistenceWindow window = new MappedPersistenceWindow( 0, 8, 16, channel, READ_WRITE );

        window.markAsInUse();
        window.lock( OperationType.WRITE );
        window.unLock();

        // when
        boolean wasClosed = window.writeOutAndCloseIfFree( false );
        file.close();

        // then
        assertTrue( wasClosed );
    }
}
