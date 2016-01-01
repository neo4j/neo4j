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

import org.neo4j.helpers.Factory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.unsafe.impl.batchimport.store.io.Monitor;
import org.neo4j.unsafe.impl.batchimport.store.io.SimplePool;

import static java.lang.Math.min;
import static java.nio.ByteBuffer.allocateDirect;

/**
 * {@link WindowPoolFactory} that is optimized for single threaded batching access.
 * There's only one (pretty big) window and it moves with the supposedly sequential access when its boundaries are
 * crossed. As part of moving the window the written data in the window (residing in a buffer) is written
 * to the channel.
 */
public class BatchingWindowPoolFactory implements WindowPoolFactory
{
    /* Used for zeroing the data in a buffer. This is a direct buffer and the target buffer is
     * also direct, so put(ByteBuffer) will result in unsafe memory copy. */
    private static final ByteBuffer ZEROS = ByteBuffer.allocateDirect( 1024 * 4 );

    public interface WriterFactory
    {
        Writer create( StoreChannel channel, Monitor monitor );

        void awaitEverythingWritten();

        void shutdown();
    }

    /**
     * Receives requests to write data to the underlying channel.
     */
    public interface Writer
    {
        void write( ByteBuffer byteBuffer, long position, SimplePool<ByteBuffer> pool ) throws IOException;
    }

    public static final WriterFactory SYNCHRONOUS = new WriterFactory()
    {
        @Override
        public Writer create( final StoreChannel channel, final Monitor monitor )
        {
            return new Writer()
            {
                @Override
                public void write( ByteBuffer data, long position, SimplePool<ByteBuffer> pool ) throws IOException
                {
                    try
                    {
                        int written = channel.write( data, position );
                        monitor.dataWritten( written );
                    }
                    finally
                    {
                        pool.release( data );
                    }
                }
            };
        }

        @Override
        public void awaitEverythingWritten()
        {   // no-op
        }

        @Override
        public void shutdown()
        {   // no-op
        }
    };

    public static enum Mode
    {
        APPEND_ONLY
                {
                    @Override
                    boolean canReadFrom( long windowIndex )
                    {
                        return windowIndex == 0;
                    }
                },
        UPDATE
                {
                    @Override
                    boolean canReadFrom( long windowIndex )
                    {
                        return true;
                    }
                };

        abstract boolean canReadFrom( long windowIndex );
    }

    private final int windowTargetSize;
    private final Monitor monitor;
    private Mode mode;
    private final WriterFactory writerFactory;

    public BatchingWindowPoolFactory( int windowTargetSize, Monitor monitor, Mode mode,
                                      WriterFactory writerFactory )
    {
        this.windowTargetSize = windowTargetSize;
        this.monitor = monitor;
        this.mode = mode;
        this.writerFactory = writerFactory;
    }

    void setMode( Mode mode )
    {
        this.mode = mode;
    }

    @Override
    public WindowPool create( File storageFileName, int recordSize, StoreChannel fileChannel, Config configuration,
                              StringLogger log, int numberOfReservedLowIds )
    {
        return new SingleWindowPool( storageFileName, recordSize, fileChannel );
    }

    private class SingleWindowPool implements WindowPool
    {
        private final SingleWindow window;
        private final File storageFileName;

        public SingleWindowPool( File storageFileName, int recordSize, StoreChannel channel )
        {
            this.storageFileName = storageFileName;
            this.window = createSingleWindow( storageFileName, recordSize, channel );
            window.allocateBuffer();
            window.placeWindowFor( 0 );
        }

        @Override
        public String toString()
        {
            return storageFileName.getName();
        }

        @Override
        public PersistenceWindow acquire( long position, OperationType operationType )
        {
            // Will change and return the single window
            return window.acquire( position, operationType );
        }

        @Override
        public void release( PersistenceWindow window )
        {   // No releasing
        }

        @Override
        public void flushAll()
        {
            window.force();
        }

        @Override
        public void close()
        {
            window.close();
        }

        @Override
        public WindowPoolStats getStats()
        {
            // TODO return anything here?
            return null;
        }
    }

    protected SingleWindow createSingleWindow( File storageFileName, int recordSize, StoreChannel channel )
    {
        return new SingleWindow( storageFileName, recordSize, channel );
    }

    protected class SingleWindow implements PersistenceWindow
    {
        private final File storageFileName;
        private final int recordSize;
        private final StoreChannel channel;
        private SimplePool<ByteBuffer> bufferPool;
        private Buffer currentBuffer;
        private int maxRecordsInBuffer;
        protected long firstIdInWindow;
        protected long lastIdInWindow;
        private final Writer writer;
        private long currentWindowIndex = -1;

        protected SingleWindow( File storageFileName, int recordSize, StoreChannel channel )
        {
            this.storageFileName = storageFileName;
            this.recordSize = recordSize;
            this.channel = channel;
            this.writer = writerFactory.create( channel, monitor );
        }

        @Override
        public String toString()
        {
            return "Batch friendly " + getClass().getSimpleName() + " for " + storageFileName;
        }

        protected void allocateBuffer()
        {
            // TODO 4k align instead
            final int capacity = roundedToNearestRecordSize( windowTargetSize );
            this.maxRecordsInBuffer = capacity / recordSize; // It's even at this point
            bufferPool = new SimplePool<>( 2, new Factory<ByteBuffer>()
            {
                @Override
                public ByteBuffer newInstance()
                {
                    return allocateDirect( capacity );
                }
            } );
            this.currentBuffer = new Buffer( this, bufferPool.acquire() );
        }

        private int roundedToNearestRecordSize( int targetSize )
        {
            int rest = targetSize % recordSize;
            return targetSize - rest;
        }

        protected PersistenceWindow acquire( long id, OperationType operationType )
        {
            assert operationType == OperationType.WRITE ||
                    (operationType == OperationType.READ && mode.canReadFrom( windowIndex( id ) ));

            boolean isInCurrentWindow = idIsWithinCurrentWindow( id );
            if ( !isInCurrentWindow )
            {
                writeBufferToChannel();
                placeWindowFor( id );
            }
            return this;
        }

        private void placeWindowFor( long id )
        {
            long windowIndex = windowIndex( id );
            firstIdInWindow = windowIndex * maxRecordsInBuffer;
            lastIdInWindow = firstIdInWindow + maxRecordsInBuffer - 1;

            // If we're not in append-only mode, i.e. if we're in update mode
            // OR if this is the first window index we read the contents.
            // The reason for reading the first windows is that in order to play nicely with
            // NeoStore and loading the store sometimes header information needs to be read,
            // even if we're in append-only mode
            if ( mode.canReadFrom( windowIndex ) )
            {
                readBufferFromChannel();
            }
            else
            {
                zeroBuffer();
            }
            currentWindowIndex = windowIndex;
            // buffer position after we placed the window is irrelevant since every future access
            // will set offset explicitly before use.
        }

        private void zeroBuffer()
        {
            // Duplicate for thread safety
            ByteBuffer zeros = ZEROS.duplicate();
            currentBuffer.reset();
            ByteBuffer buffer = currentBuffer.getBuffer();

            while ( buffer.hasRemaining() )
            {
                int chunkSize = min( buffer.remaining(), zeros.capacity() );
                zeros.clear();
                zeros.limit( chunkSize );
                buffer.put( zeros );
            }
        }

        private long windowIndex( long id )
        {
            return id / maxRecordsInBuffer;
        }

        private boolean idIsWithinCurrentWindow( long id )
        {
            return windowIndex( id ) == currentWindowIndex;
        }

        @Override
        public Buffer getBuffer()
        {
            throw new UnsupportedOperationException( "Not really needed" );
        }

        @Override
        public Buffer getOffsettedBuffer( long id )
        {
            assert idIsWithinCurrentWindow( id ) : "Quite surprisingly the id " + id +
                    " is outside the current window. At this point acquire should have been called previously" +
                    " with the same id. First id in window " + firstIdInWindow + ", last " + lastIdInWindow;

            currentBuffer.setOffset( (int) ((id - firstIdInWindow) * recordSize) );
            return currentBuffer;
        }

        @Override
        public int getRecordSize()
        {
            return recordSize;
        }

        @Override
        public long position()
        {
            try
            {
                return channel.position();
            }
            catch ( IOException e )
            {
                throw handleIoException( e );
            }
        }

        private RuntimeException handleIoException( IOException e )
        {
            throw new RuntimeException( e );
        }

        @Override
        public int size()
        {
            throw new UnsupportedOperationException( "Not needed" );
        }

        @Override
        public void force()
        {
            writeBufferToChannel();
        }

        private void writeBufferToChannel()
        {
            if ( currentWindowIndex == -1 )
            {
                return;
            }

            try
            {
                writer.write( prepared( currentBuffer.getBuffer() ), firstIdInWindow * recordSize, bufferPool );
                currentBuffer = new Buffer( this, bufferPool.acquire() );
                currentBuffer.reset();
                currentWindowIndex = -1;
            }
            catch ( IOException e )
            {
                throw handleIoException( e );
            }
        }

        private void readBufferFromChannel()
        {
            try
            {
                channel.read( prepared( currentBuffer.getBuffer() ), firstIdInWindow * recordSize );
            }
            catch ( IOException e )
            {
                throw handleIoException( e );
            }
        }

        private ByteBuffer prepared( ByteBuffer buffer )
        {
            buffer.flip();
            buffer.limit( (int) ((lastIdInWindow - firstIdInWindow + 1) * recordSize) );
            return buffer;
        }

        @Override
        public void close()
        {
            force();
        }
    }
}
