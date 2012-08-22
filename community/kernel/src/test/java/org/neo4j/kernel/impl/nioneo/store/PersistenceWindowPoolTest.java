/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.Exceptions.launderedException;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.ResourceCollection;
import org.neo4j.test.TargetDirectory;

public class PersistenceWindowPoolTest
{
    private static final TargetDirectory target = TargetDirectory.forTest( MappedPersistenceWindowTest.class );
    @Rule
    public final TargetDirectory.TestDirectory directory = target.testDirectory();
    @Rule
    public final ResourceCollection resources = new ResourceCollection();

    @Test
    public void shouldBeAbleToReAcquireReleasedWindow() throws Exception
    {
        // given
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        PersistenceWindowPool pool = new PersistenceWindowPool( "test.store", 8, file.getChannel(), 0, false, false );

        PersistenceWindow initialWindow = pool.acquire( 0, OperationType.READ );
        pool.release( initialWindow );

        // when
        PersistenceWindow window = pool.acquire( 0, OperationType.READ );

        // then
        assertNotSame( initialWindow, window );
    }
    
    @Test
    public void handOverDirtyPersistenceRowToReaderShouldWriteWhenClosing() throws Exception
    {
        String filename = new File( target.graphDbDir( true ), "dirty" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        final int blockSize = 8;
        final PersistenceWindowPool pool = new PersistenceWindowPool( "test.store", blockSize, file.getChannel(), 0, false, false );
        
        // The gist:
        // T1 acquires position 0 as WRITE
        // T2 would like to acquire position 0 as READ, marks it and goes to wait in lock()
        // T1 writes stuff to the buffer and releases it
        // T2 gets the PR handed over from T1, reads and verifies that it got the changes made by T1
        // T2 releases it
        // Verify that what T1 wrote is on disk
        
        final PersistenceWindow t1Row = pool.acquire( 0, OperationType.WRITE );
        OtherThreadExecutor<Void> otherThread = new OtherThreadExecutor<Void>( null ); 
        Future<Throwable> future = otherThread.executeDontWait( new WorkerCommand<Void, Throwable>()
        {
            @Override
            public Throwable doWork( Void state )
            {
                PersistenceWindow t2Row = pool.acquire( 0, OperationType.READ ); // Will block until t1Row is released.
                try
                {
                    assertTrue( t1Row == t2Row );
                    assertBufferContents( blockSize, t2Row );
                    return null;
                }
                catch ( Throwable t )
                {
                    return t;
                }
                finally
                {
                    pool.release( t2Row );
                }
            }
        } );
        try
        {
            writeBufferContents( blockSize, t1Row );
            otherThread.waitUntilWaiting();
        }
        finally
        {
            pool.release( t1Row );
        }
        Throwable failure = future.get();
        if ( failure != null )
            throw launderedException( failure );
        
        PersistenceWindow row = pool.acquire( 0, OperationType.READ );
        assertFalse( t1Row == row );
        assertBufferContents( blockSize, row );
        
        pool.close();
        otherThread.shutdown();
    }

    private void writeBufferContents( final int blockSize, final PersistenceWindow t1Row )
    {
        Buffer buffer = t1Row.getBuffer();
        for ( int i = 0; i < blockSize; i++ )
            buffer.put( (byte) i );
    }

    private void assertBufferContents( final int blockSize, PersistenceWindow row )
    {
        Buffer buffer = row.getBuffer();
        for ( int i = 0; i < blockSize; i++ )
            assertEquals( (byte)i, buffer.get() );
    }
}
