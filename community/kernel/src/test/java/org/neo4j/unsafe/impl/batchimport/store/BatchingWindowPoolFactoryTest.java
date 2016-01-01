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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel.MapMode;
import java.nio.channels.FileLock;
import java.util.Arrays;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.test.EphemeralFileSystemRule;
import org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.Mode;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;

import static java.nio.ByteBuffer.wrap;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.unsafe.impl.batchimport.store.BatchingWindowPoolFactory.SYNCHRONOUS;

public class BatchingWindowPoolFactoryTest
{
    @Test
    public void shouldOnlyReadFirstWindowInAppendOnlyMode() throws Exception
    {
        // GIVEN
        byte[] someBytes = new byte[] { 1, 2, 3, 4, 5 };
        // some data in the first window
        prepopulateChannel( data( 0, 0, someBytes ) );
        // some data in the second window
        prepopulateChannel( data( 1, 0, someBytes ) );
        Monitor monitor = mock( Monitor.class );
        WindowPool pool = pool( monitor, Mode.APPEND_ONLY );

        // WHEN
        PersistenceWindow window = pool.acquire( recordId( 0, 0 ), OperationType.READ );
        Buffer buffer = window.getOffsettedBuffer( 0 );
        byte[] readData = new byte[someBytes.length];
        buffer.get( readData );
        assertArrayEquals( someBytes, readData );

        try
        {
            pool.acquire( recordId( 1, 0 ), OperationType.READ );
            fail( "Shouldn't be able to read from window other than 0" );
        }
        catch ( Throwable e )
        {   // Actually this is most likely going to be an AssertionError, but it's OK
        }
    }

    @Test
    public void shouldReadAnyRecordInUpdateMode() throws Exception
    {
        // GIVEN
        byte[] someBytes = new byte[] { 1, 2, 3, 4, 5 };
        byte[] someOtherBytes = new byte[] { 6, 7, 8, 9, 10 };
        // some data in the first window
        prepopulateChannel( data( 0, 0, someBytes ) );
        // some data in the second window
        prepopulateChannel( data( 1, 0, someOtherBytes ) );
        Monitor monitor = mock( Monitor.class );
        WindowPool pool = pool( monitor, Mode.UPDATE );

        // WHEN
        {
            PersistenceWindow window = pool.acquire( recordId( 0, 0 ), OperationType.READ );
            Buffer buffer = window.getOffsettedBuffer( 0 );
            byte[] readData = new byte[someBytes.length];
            buffer.get( readData );
            assertArrayEquals( someBytes, readData );
        }
        {
            PersistenceWindow window = pool.acquire( recordId( 1, 0 ), OperationType.READ );
            Buffer buffer = window.getOffsettedBuffer( recordId( 1, 0 ) );
            byte[] readData = new byte[someBytes.length];
            buffer.get( readData );
            assertArrayEquals( someOtherBytes, readData );
        }
    }

    @Test
    public void shouldWriteWindowChangesToChannelBeforeWhenMovingIt() throws Exception
    {
        // GIVEN
        byte[] someBytes = new byte[] { 1, 2, 3, 4, 5 };
        byte[] someOtherBytes = new byte[] { 6, 7, 8, 9, 10 };
        Monitor monitor = mock( Monitor.class );
        WindowPool pool = pool( monitor, Mode.APPEND_ONLY );

        // WHEN
        {
            PersistenceWindow window = pool.acquire( recordId( 0, 0 ), OperationType.WRITE );
            window.getOffsettedBuffer( recordId( 0, 0 ) ).put( someBytes );
            verify( monitor, times( 0 ) ).dataWritten( anyInt() );
        }

        {
            PersistenceWindow window = pool.acquire( recordId( 0, 3 ), OperationType.WRITE );
            window.getOffsettedBuffer( recordId( 0, 3 ) ).put( someOtherBytes );
            verify( monitor, times( 0 ) ).dataWritten( anyInt() );
        }

        // THEN
        {
            PersistenceWindow window = pool.acquire( recordId( 1, 0 ), OperationType.WRITE );
            // here the window should have moved
            verify( monitor, times( 1 ) ).dataWritten( anyInt() );
        }
        verifyData( recordId( 0, 0 ), someBytes );
        verifyData( recordId( 0, 3 ), someOtherBytes );
    }

    @Test
    public void shouldZeroOutWindowBetweenUses() throws Exception
    {
        // GIVEN
        byte[] someBytes = new byte[]{1, 2, 3, 4, 5};
        Monitor monitor = mock( Monitor.class );
        WindowPool pool = pool( monitor, Mode.APPEND_ONLY );

        // WHEN
        {
            PersistenceWindow window = pool.acquire( recordId( 1, 2 ), OperationType.WRITE );
            window.getOffsettedBuffer( recordId( 1, 2 ) ).put( someBytes );
        }

        PersistenceWindow window = pool.acquire( recordId( 2, 2 ), OperationType.WRITE );
        byte[] readBack = new byte[someBytes.length];
        window.getOffsettedBuffer( recordId( 2, 2 ) ).get( readBack );

        byte[] zeros = new byte[someBytes.length];
        Arrays.fill( zeros, (byte) 0 );

        assertArrayEquals( zeros, readBack );
    }

    @Test
    public void shouldFlushWhenToldTo() throws Exception
    {
        // GIVEN
        Monitor monitor = mock( Monitor.class );
        WindowPool pool = pool( monitor, Mode.APPEND_ONLY );

        PersistenceWindow window = pool.acquire( 0, OperationType.WRITE );
        try
        {
            Buffer buffer = window.getOffsettedBuffer( 0 );
            buffer.putInt( 1234 );
        }
        finally
        {
            pool.release( window );
        }

        // WHEN
        pool.flushAll();

        // THEN
        verify( monitor ).dataWritten( windowSize );
    }

    private void verifyData( long recordId, byte[] expectedData ) throws IOException
    {
        try ( StoreChannel channel = openChannel() )
        {
            channel.position( recordPosition( recordId ) );
            byte[] readData = new byte[expectedData.length];
            channel.read( wrap( readData ) );
            assertArrayEquals( expectedData, readData );
        }
    }

    private long recordPosition( long recordId )
    {
        return recordId*recordSize;
    }

    private long recordId( int windowIndex, int recordInWindowIndex )
    {
        return windowIndex*recordsPerWindow + recordInWindowIndex;
    }

    private void prepopulateChannel( Data... datas ) throws IOException
    {
        try ( StoreChannel channel = openChannel() )
        {
            for ( Data data : datas )
            {
                data.writeTo( channel );
            }
        }
    }

    private StoreChannel openChannel() throws IOException
    {
        return fs.get().open( file, "rw" );
    }

    private Data data( int windowIndex, int recordInWindowIndex, byte[] someBytes )
    {
        return new Data( recordId( windowIndex, recordInWindowIndex )*recordSize, someBytes );
    }

    private WindowPool pool( Monitor monitor, Mode mode ) throws IOException
    {
        channel = new TrackingStoreChannel( openChannel() );
        WindowPoolFactory factory = new BatchingWindowPoolFactory( windowSize, monitor, mode, SYNCHRONOUS );
        return factory.create( file, recordSize, channel, new Config(), StringLogger.DEV_NULL, 0 );
    }

    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    private final int recordSize = 20;
    private final int recordsPerWindow = 10;
    private final int windowSize = recordSize*recordsPerWindow;
    private final File file = new File( "store" );
    private TrackingStoreChannel channel;

    @After
    public void after() throws IOException
    {
        channel.close();
    }

    private static class TrackingStoreChannel implements StoreChannel
    {
        private final StoreChannel delegate;

        public TrackingStoreChannel( StoreChannel delegate )
        {
            this.delegate = delegate;
        }

        @Override
        public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
        {
            return delegate.read( dsts, offset, length );
        }

        @Override
        public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
        {
            return delegate.write( srcs, offset, length );
        }

        @Override
        public FileLock tryLock() throws IOException
        {
            return delegate.tryLock();
        }

        @Override
        public boolean isOpen()
        {
            return delegate.isOpen();
        }

        @Override
        public void close() throws IOException
        {
            delegate.close();
        }

        @Override
        public int read( ByteBuffer dst ) throws IOException
        {
            return delegate.read( dst );
        }

        @Override
        public int write( ByteBuffer src, long position ) throws IOException
        {
            return delegate.write( src, position );
        }

        @Override
        public MappedByteBuffer map( MapMode mode, long position, long size ) throws IOException
        {
            return delegate.map( mode, position, size );
        }

        @Override
        public int read( ByteBuffer dst, long position ) throws IOException
        {
            return delegate.read( dst, position );
        }

        @Override
        public void force( boolean metaData ) throws IOException
        {
            delegate.force( metaData );
        }

        @Override
        public StoreChannel position( long newPosition ) throws IOException
        {
            return delegate.position( newPosition );
        }

        @Override
        public int write( ByteBuffer src ) throws IOException
        {
            return delegate.write( src );
        }

        @Override
        public StoreChannel truncate( long size ) throws IOException
        {
            return delegate.truncate( size );
        }

        @Override
        public long position() throws IOException
        {
            return delegate.position();
        }

        @Override
        public long read( ByteBuffer[] dsts ) throws IOException
        {
            return delegate.read( dsts );
        }

        @Override
        public long size() throws IOException
        {
            return delegate.size();
        }

        @Override
        public long write( ByteBuffer[] srcs ) throws IOException
        {
            return delegate.write( srcs );
        }

        @Override
        public void writeAll( ByteBuffer src, long position ) throws IOException
        {
            delegate.writeAll( src, position );
        }

        @Override
        public void writeAll( ByteBuffer src ) throws IOException
        {
            delegate.writeAll( src );
        }
    }

    public class Data
    {
        private final long position;
        private final byte[] data;

        public Data( long position, byte[] data )
        {
            this.position = position;
            this.data = data;
        }

        public void writeTo( StoreChannel channel ) throws IOException
        {
            channel.position( position );
            channel.write( wrap( data ) );
        }
    }
}
