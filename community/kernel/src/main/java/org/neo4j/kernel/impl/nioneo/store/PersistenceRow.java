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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * PersistenRow only encapsulates one record in a store. It is used as a light-
 * weight window when no other (larger window) is found that encapsulates the
 * required record/block and it would be non efficient to create a large new
 * window to perform the required operation.
 */
class PersistenceRow extends LockableWindow
{
    private State bufferState = State.EMPTY;
    private int recordSize = -1;
    private final long position;
    private final Buffer buffer;

    PersistenceRow( long position, int recordSize, FileChannel channel )
    {
        super( channel );
        assert position >= 0 : "Illegal position[" + position + "]";
        assert recordSize > 0 : "Illegal recordSize[" + recordSize + "]";
        assert channel != null : "Null file channel";

        this.position = position;
        this.recordSize = recordSize;
        this.buffer = new Buffer( this, ByteBuffer.allocate( recordSize ) );
        markAsInUse();
    }

    @Override
    void lock( OperationType operationType )
    {
        super.lock( operationType );
        boolean success = false;
        try
        {
            bufferState = bufferState.transition( operationType, this );
            success = true;
        }
        finally
        {
            if ( !success )
            {
                unLock();
            }
        }
    }

    public boolean isDirty()
    {
        return bufferState == State.DIRTY;
    }

    @Override
    public Buffer getBuffer()
    {
        return buffer;
    }
    
    @Override
    public int getRecordSize()
    {
        return recordSize;
    }

    @Override
    public Buffer getOffsettedBuffer( long id )
    {
        if ( id != buffer.position() )
        {
            throw new InvalidRecordException( "Id[" + id + 
                "] not equal to buffer position[" + buffer.position() + "]" );
        }
        return buffer;
    }
    
    @Override
    public long position()
    {
        return position;
    }

    private static enum State
    {
        EMPTY
        {
            @Override
            State transition( OperationType operationType, PersistenceRow persistenceRow )
            {
                switch ( operationType)
                {
                    case READ:
                        persistenceRow.readFullWindow();
                        return CLEAN;
                    case WRITE:
                        return DIRTY;
                    default:
                        throw new IllegalStateException( "Unknown operation type: " + operationType );
                }
            }
        },
        CLEAN
        {
            @Override
            State transition( OperationType operationType, PersistenceRow persistenceRow )
            {
                switch ( operationType)
                {
                    case READ:
                        return CLEAN;
                    case WRITE:
                        return DIRTY;
                    default:
                        throw new IllegalStateException( "Unknown operation type: " + operationType );
                }
            }
        },
        DIRTY
        {
            @Override
            State transition( OperationType operationType, PersistenceRow persistenceRow )
            {
                return DIRTY;
            }
        };

        abstract State transition( OperationType operationType, PersistenceRow persistenceRow );
    }

    void readFullWindow()
    {
        try
        {
            ByteBuffer byteBuffer = buffer.getBuffer();
            byteBuffer.clear();
            getFileChannel().read( byteBuffer, position * recordSize );
            byteBuffer.clear();
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to load position["
                + position + "] @[" + position * recordSize + "]", e );
        }
    }

    private void writeContents()
    {
        if ( isDirty() )
        {
            ByteBuffer byteBuffer = buffer.getBuffer().duplicate();
            byteBuffer.clear();
            try
            {
                int written = 0;

                while ( byteBuffer.hasRemaining() )
                {
                    int writtenThisTime = getFileChannel().write( byteBuffer, position * recordSize + written );

                    if ( writtenThisTime == 0 )
                    {
                        throw new IOException( "Unable to write to disk, reported bytes written was 0" );
                    }

                    written += writtenThisTime;
                }
            }
            catch ( IOException e )
            {
                throw new UnderlyingStorageException( "Unable to write record["
                        + position + "] @[" + position * recordSize + "]", e );
            }
        }
    }

    @Override
    public int size()
    {
        return 1;
    }

    @Override
    public void force()
    {
        writeContents();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !(o instanceof PersistenceRow) )
        {
            return false;
        }
        return position() == ((PersistenceRow) o).position();
    }

    @Override
    public int hashCode()
    {
        return (int) this.position;
    }

    @Override
    public String toString()
    {
        return "PersistenceRow[" + position + "]";
    }

    @Override
    public synchronized void close()
    {
        buffer.close();
        closed = true;
    }

    public void reset()
    {
        buffer.reset();
    }
}