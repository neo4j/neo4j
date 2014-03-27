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

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.Buffer;
import org.neo4j.kernel.impl.nioneo.store.OperationType;
import org.neo4j.kernel.impl.nioneo.store.PersistenceWindow;
import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.impl.nioneo.store.WindowPoolStats;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPool;
import org.neo4j.kernel.impl.nioneo.store.windowpool.WindowPoolFactory;
import org.neo4j.kernel.impl.util.StringLogger;

import static java.nio.ByteBuffer.allocateDirect;

/**
 * {@link WindowPoolFactory} that is optimized for going through a store just writing new records. No data
 * is read in this implementation. There's only one (pretty big) window and it moves when the upper id boundary
 * is crossed. As part of moving the window the written data in the window (residing in a buffer) is written
 * to the channel.
 */
public class BatchWritingWindowPoolFactory implements WindowPoolFactory
{
    private final int bufferTargetSize;
    private final Monitor monitor;

    public BatchWritingWindowPoolFactory( int bufferTargetSize, Monitor monitor )
    {
        this.bufferTargetSize = bufferTargetSize;
        this.monitor = monitor;
    }

    @Override
    public WindowPool create( File storageFileName, int recordSize, StoreChannel channel, Config configuration,
            StringLogger log, int numberOfReservedLowIds )
    {
        return new Pool( storageFileName, recordSize, channel, numberOfReservedLowIds );
    }

    private class Pool implements WindowPool
    {
        private final Window window;
        private final File storageFileName;

        public Pool( File storageFileName, int recordSize, StoreChannel channel,
                int numberOfReservedLowIds )
        {
            this.storageFileName = storageFileName;
            this.window = new Window( storageFileName, recordSize, channel,
                    numberOfReservedLowIds );
            this.window.allocateBuffer();
        }

        @Override
        public String toString()
        {
            return storageFileName.getName();
        }

        @Override
        public PersistenceWindow acquire( long position, OperationType operationType )
        {
            return window.acquire( operationType );
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
            // This happens when closing the store
            window.close();
        }

        @Override
        public WindowPoolStats getStats()
        {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private class Window implements PersistenceWindow
    {
        private final File storageFileName;
        private final int recordSize;
        private final StoreChannel channel;
        private Buffer reusableBuffer;
        private int recordsInBuffer;
        private long firstIdInWindow;
        private long highestSeenId;
        private OperationType operationType;

        public Window( File storageFileName, int recordSize,
                StoreChannel fileChannel, int numberOfReservedLowIds )
        {
            this.storageFileName = storageFileName;
            this.channel = fileChannel;
            this.recordSize = recordSize;
            positionAtRecord( numberOfReservedLowIds );
            this.highestSeenId = firstIdInWindow-1;
        }

        @Override
        public String toString()
        {
            return storageFileName.getName();
        }

        public PersistenceWindow acquire( OperationType operationType )
        {
            this.operationType = operationType;
            return this;
        }

        private void positionAtRecord( int record )
        {
            try
            {
                channel.position( recordSize*record );
                firstIdInWindow = record;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        public void allocateBuffer()
        {
            this.reusableBuffer = new Buffer( this, allocateDirect( roundedToNearestRecordSize( bufferTargetSize ) ) );
            this.recordsInBuffer = reusableBuffer.getBuffer().capacity() / recordSize; // It's even at this point
        }

        private int roundedToNearestRecordSize( int targetSize )
        {
            int rest = targetSize % recordSize;
            return targetSize - rest;
        }

        @Override
        public Buffer getBuffer()
        {
            throw new UnsupportedOperationException( "Let's see if this is needed shall we?" );
        }

        @Override
        public Buffer getOffsettedBuffer( long id )
        {
            if ( operationType == OperationType.WRITE )
            {
                assert id > highestSeenId : "Cannot go backwards in id sequence, highest seen " + highestSeenId +
                        ", givem id " + id;
                while ( forceBufferIfOutsideCurrentWindow( id ) )
                {   // Move to the right window
                }
                highestSeenId = id;
            }
            // READing should only occur on very low record ids and only at creation/startup the first time

            reusableBuffer.setOffset( (int) ((id-firstIdInWindow) * recordSize) );
            return reusableBuffer;
        }

        private boolean idIsWithinCurrentWindow( long id )
        {
            boolean miss = (id-firstIdInWindow) +1/*since both are inclusive*/ >= recordsInBuffer;
            return !miss;
        }

        private boolean forceBufferIfOutsideCurrentWindow( long id )
        {
            if ( !idIsWithinCurrentWindow( id ) )
            {
                force();
                return true;
            }
            return false;
        }

        @Override
        public int getRecordSize()
        {
            return recordSize;
        }

        @Override
        public long position()
        {   // Regard me as one big persistence window.
            try
            {
                return channel.position();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        @Override
        public int size()
        {   // Don't think this is ever used actually
            return 0;
        }

        @Override
        public void force()
        {
            try
            {
                int bytesWritten = channel.write( prepared( reusableBuffer.getBuffer() ) );
                monitor.dataWritten( bytesWritten );
                reusableBuffer.reset();
                firstIdInWindow = highestSeenId+1;
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }

        private ByteBuffer prepared( ByteBuffer buffer )
        {
            buffer.flip();
            buffer.limit( (int) ((highestSeenId-firstIdInWindow+1) * recordSize) );
            return buffer;
        }

        @Override
        public void close()
        {
            force();
        }
    }
}
