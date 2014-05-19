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
package org.neo4j.unsafe.impl.batchimport.store;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.monitoring.Monitors;

import static java.nio.ByteBuffer.allocateDirect;

/**
 * {@link WindowPoolFactory} that is optimized for single threaded batching access.
 * There's only one (pretty big) window and it moves with the supposedly sequential access when its boundaries are
 * crossed. As part of moving the window the written data in the window (residing in a buffer) is written
 * to the channel.
 */
public class BatchFriendlyWindowPoolFactory implements WindowPoolFactory
{
    public interface WriterFactory
    {
        Writer create( File file, StoreChannel channel, Monitor monitor );
    }

    /**
     * Receives requests to write data to the underlying channel.
     */
    public interface Writer
    {
        void write( ByteBuffer byteBuffer, long position ) throws IOException;
    }

    public static final WriterFactory SYNCHRONOUS = new WriterFactory()
    {
        @Override
        public Writer create( File file, final StoreChannel channel, final Monitor monitor )
        {
            return new Writer()
            {
                @Override
                public void write( ByteBuffer data, long position ) throws IOException
                {
                    channel.position( position );
                    int written = channel.write( data );
                    monitor.dataWritten( written );
                }
            };
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
    private final Mode mode;
    private final WriterFactory writerFactory;

    public BatchFriendlyWindowPoolFactory( int windowTargetSize, Monitor monitor, Mode mode,
            WriterFactory writerFactory )
    {
        this.windowTargetSize = windowTargetSize;
        this.monitor = monitor;
        this.mode = mode;
        this.writerFactory = writerFactory;
    }

    @Override
    public WindowPool create( File storageFileName, int recordSize, StoreChannel fileChannel, Config configuration,
            int numberOfReservedLowIds, Monitors monitors )
    {
        return new SingleWindowPool( storageFileName, recordSize, fileChannel, numberOfReservedLowIds );
    }

    private class SingleWindowPool implements WindowPool
    {
        private final SingleWindow window;
        private final File storageFileName;

        public SingleWindowPool( File storageFileName, int recordSize, StoreChannel channel,
                int numberOfReservedLowIds )
        {
            this.storageFileName = storageFileName;
            this.window = createSingleWindow( storageFileName, recordSize, channel,
                    numberOfReservedLowIds );
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
        {   // Nah
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

    protected SingleWindow createSingleWindow( File storageFileName, int recordSize, StoreChannel channel,
            int numberOfReservedLowIds )
    {
        return new SingleWindow( storageFileName, recordSize, channel );
    }

    protected class SingleWindow implements PersistenceWindow
    {
        private final File storageFileName;
        private final int recordSize;
        private final StoreChannel channel;
        private Buffer reusableBuffer;
        private int maxRecordsInBuffer;
        protected long firstIdInWindow;
        protected long lastIdInWindow;
        private final Writer writer;

        protected SingleWindow( File storageFileName, int recordSize, StoreChannel channel )
        {
            this.storageFileName = storageFileName;
            this.recordSize = recordSize;
            this.channel = channel;
            this.writer = writerFactory.create( storageFileName, channel, monitor );
        }

        @Override
        public String toString()
        {
            return "Batch friendly " + getClass().getSimpleName() + " for " + storageFileName;
        }

        protected void allocateBuffer()
        {
            // TODO 4k align instead
            this.reusableBuffer = new Buffer( this, allocateDirect( roundedToNearestRecordSize( windowTargetSize ) ) );
            this.maxRecordsInBuffer = reusableBuffer.getBuffer().capacity() / recordSize; // It's even at this point
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
            firstIdInWindow = windowIndex*maxRecordsInBuffer;
            lastIdInWindow = firstIdInWindow+maxRecordsInBuffer-1;

            // If we're not in append-only mode, i.e. if we're in update mode
            // OR if this is the first window index we read the contents.
            // The reason for reading the first windows is that in order to play nicely with
            // NeoStore and loading the store sometimes header information needs to be read,
            // even if we're in append-only mode
            if ( mode.canReadFrom( windowIndex ) )
            {
                readBufferFromChannel();
            }
        }

        private long windowIndex( long id )
        {
            return id/maxRecordsInBuffer;
        }

        private boolean idIsWithinCurrentWindow( long id )
        {
            return id >= firstIdInWindow && id <= lastIdInWindow;
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

            reusableBuffer.setOffset( (int) ((id-firstIdInWindow) * recordSize) );
            return reusableBuffer;
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
            try
            {
                writer.write( prepared( reusableBuffer.getBuffer() ), firstIdInWindow*recordSize );
                reusableBuffer.reset();
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
                channel.position( firstIdInWindow*recordSize );
                channel.read( prepared( reusableBuffer.getBuffer() ) );
            }
            catch ( IOException e )
            {
                throw handleIoException( e );
            }
        }

        private ByteBuffer prepared( ByteBuffer buffer )
        {
            buffer.flip();
            buffer.limit( (int) ((lastIdInWindow-firstIdInWindow+1) * recordSize) );
            return buffer;
        }

        @Override
        public void close()
        {
            force();
        }
    }
}
