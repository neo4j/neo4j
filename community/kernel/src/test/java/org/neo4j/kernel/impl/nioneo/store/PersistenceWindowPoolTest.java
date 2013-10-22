/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.ResourceCollection;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.Exceptions.launderedException;

public class PersistenceWindowPoolTest
{
    private static final TargetDirectory target = TargetDirectory.forTest( MappedPersistenceWindowTest.class );
    @Rule
    public final ResourceCollection resources = new ResourceCollection();
    @Rule
    public final TargetDirectory.TestDirectory directory = target.testDirectory();
    @Rule
    public final ExpectedException expectedUnderlyingException = ExpectedException.none();

    @Test
    public void shouldBeAbleToReAcquireReleasedWindow() throws Exception
    {
        // given
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        PersistenceWindowPool pool = new PersistenceWindowPool( new File("test.store"), 8,
                file.getChannel(), 0, false, false, new ConcurrentHashMap<Long, PersistenceRow>(),
                BrickElementFactory.DEFAULT, StringLogger.DEV_NULL );

        PersistenceWindow initialWindow = pool.acquire( 0, OperationType.READ );
        pool.release( initialWindow );

        // when
        PersistenceWindow window = pool.acquire( 0, OperationType.READ );

        // then
        assertNotSame( initialWindow, window );
        
        pool.close();
        file.close();
    }

    @Test
    public void handOverDirtyPersistenceRowToReaderShouldWriteWhenClosing() throws Exception
    {
        String filename = new File( target.graphDbDir( true ), "dirty" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        final int blockSize = 8;
        final PersistenceWindowPool pool = new PersistenceWindowPool( new File("test.store"), blockSize,
                file.getChannel(), 0, false, false, new ConcurrentHashMap<Long, PersistenceRow>(),
                BrickElementFactory.DEFAULT, StringLogger.DEV_NULL );
        
        // The gist:
        // T1 acquires position 0 as WRITE
        // T2 would like to acquire position 0 as READ, marks it and goes to wait in lock()
        // T1 writes stuff to the buffer and releases it
        // T2 gets the PR handed over from T1, reads and verifies that it got the changes made by T1
        // T2 releases it
        // Verify that what T1 wrote is on disk
        
        final PersistenceWindow t1Row = pool.acquire( 0, OperationType.WRITE );
        OtherThreadExecutor<Void> otherThread = new OtherThreadExecutor<Void>( "other thread", null ); 
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
        {
            throw launderedException( failure );
        }
        
        PersistenceWindow row = pool.acquire( 0, OperationType.READ );
        assertFalse( t1Row == row );
        assertBufferContents( blockSize, row );
        
        pool.close();
        otherThread.shutdown();
        file.close();
    }

    @Test
    public void releaseShouldUnlockWindowEvenIfExceptionIsThrown() throws Exception
    {
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        PersistenceWindowPool pool = new PersistenceWindowPool( new File("test.store"), 8, file.getChannel(), 0,
                false, false, new ConcurrentHashMap<Long, PersistenceRow>(), BrickElementFactory.DEFAULT,
                StringLogger.DEV_NULL );

        PersistenceRow row = mock( PersistenceRow.class );
        when( row.writeOutAndCloseIfFree( false ) ).thenThrow(
                new UnderlyingStorageException ("Unable to write record" ) );

        expectedUnderlyingException.expect( UnderlyingStorageException.class );

        try
        {
            pool.release( row );
        }
        finally
        {
            verify( row ).unLock();
        }

        pool.close();
        file.close();
    }

    @Test
    public void brickSizeZeroShouldNotCauseNPEWhenOtherThreadLoadsPersistenceRow() throws Exception
    {
        // Given
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        PersistenceRow window = new PersistenceRow( 0l, 10, file.getChannel() );

        ConcurrentMap<Long, PersistenceRow> map = mock(ConcurrentMap.class);

        // On the first lookup, pretend the row is not in memory, this makes the current thread decide to load
        // the row itself. The second time this method is called will be when the acquire routine realizes another
        // thread has loaded the window, and goes off to get that window.
        when(map.get( 0l )).then(returnNullFirstTimeButAWindowSecondTime(window));

        // TWIST! When the thread has loaded the row, it will try to insert it into the map, except now we pretend
        // another thread has already put it in there, triggering a branch where our original thread will undo any
        // locks it's grabbed as well as any memory it has allocated.
        when( map.putIfAbsent( eq( 0l ), any( PersistenceRow.class ) ) ).thenReturn( window );

        PersistenceWindowPool pool = new PersistenceWindowPool( new File("test.store"), 8, file.getChannel(), 0,
                false, false, map, BrickElementFactory.DEFAULT, StringLogger.DEV_NULL );

        // When
        PersistenceWindow acquiredWindow = pool.acquire( 0l, OperationType.READ );

        // Then
        assertEquals(window, acquiredWindow);


        pool.close();
        file.close();
    }
    
    @Test
    public void shouldSeeEqualNumberBrickLockAndUnlock() throws Exception
    {
        // GIVEN
        // -- a store file that has some records in it already
        String filename = new File( directory.directory(), "mapped.file" ).getAbsolutePath();
        RandomAccessFile file = resources.add( new RandomAccessFile( filename, "rw" ) );
        file.setLength( 8*10 );
        // -- a pool with a brick factory that tracks calls to lock/unlock
        final AtomicInteger lockedCount = new AtomicInteger(), unlockedCount = new AtomicInteger();
        BrickElementFactory brickFactory = new BrickElementFactory()
        {
            @Override
            public BrickElement create( final int index )
            {
                return new BrickElement( index )
                {
                    @Override
                    synchronized void lock()
                    {
                        assertEquals( 0, index );
                        super.lock();
                        lockedCount.incrementAndGet();
                    }
                    
                    @Override
                    void unLock()
                    {
                        assertEquals( 0, index );
                        super.unLock();
                        unlockedCount.incrementAndGet();
                    }
                };
            }
        };
        PersistenceWindowPool pool = new PersistenceWindowPool( new File("test.store"), 8,
                file.getChannel(), 10000, false, false, new ConcurrentHashMap<Long, PersistenceRow>(),
                brickFactory, StringLogger.DEV_NULL );
        
        try
        {
            // WHEN
            // -- we acquire/release a window for position 0 (which have not been mapped
            //    and will therefore be of type PersistenceRow
            pool.release( pool.acquire( 0, OperationType.READ ) );
            
            // THEN
            // -- there should have been 
            assertEquals( 1, lockedCount.get() );
            assertEquals( 1, unlockedCount.get() );
        }
        finally
        {
            pool.close();
        }
    }

    private Answer<PersistenceRow> returnNullFirstTimeButAWindowSecondTime(final PersistenceRow window)
    {
        return new Answer<PersistenceRow>()
        {
            int invocations = 0;

            @Override
            public PersistenceRow answer( InvocationOnMock invocationOnMock ) throws Throwable
            {
                if(invocations++ == 0)
                {
                    return null;
                }
                return window;
            }
        };
    }

    private void writeBufferContents( final int blockSize, final PersistenceWindow t1Row )
    {
        Buffer buffer = t1Row.getBuffer();
        for ( int i = 0; i < blockSize; i++ )
        {
            buffer.put( (byte) i );
        }
    }

    private void assertBufferContents( final int blockSize, PersistenceWindow row )
    {
        Buffer buffer = row.getBuffer();
        for ( int i = 0; i < blockSize; i++ )
        {
            assertEquals( (byte)i, buffer.get() );
        }
    }
}
