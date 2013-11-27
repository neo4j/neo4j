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

abstract class AbstractPersistenceWindow extends LockableWindow
{
    private final int recordSize;
    private final long position;
    private Buffer buffer = null;
    private final int windowSize;

    AbstractPersistenceWindow( long position, int recordSize, int totalSize, 
        FileChannel channel, ByteBuffer byteBuffer )
    {
        super( channel );
        assert position >= 0 : "Illegal position[" + position + "]";
        assert recordSize > 0 : "Illegal recordSize[" + recordSize + "]";
        assert channel != null : "Null file channel";
        assert totalSize >= recordSize;
        
        this.position = position;
        this.recordSize = recordSize;
        this.windowSize = totalSize / recordSize;
        this.buffer = new Buffer( this, byteBuffer );
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
        int offset = (int) (id - buffer.position()) * recordSize;
        buffer.setOffset( offset );
        return buffer;
    }
    
    @Override
    public long position()
    {
        return position;
    }

    void readFullWindow()
    {
        try
        {
            long fileSize = getFileChannel().size();
            long recordCount = fileSize / recordSize;
            // possible last element not written completely, therefore if
            // fileSize % recordSize can be non 0 and we check > instead of >=
            if ( position > recordCount )
            {
                // use new buffer since it will contain only zeros
                return;
            }
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
        ByteBuffer byteBuffer = buffer.getBuffer().duplicate();
        byteBuffer.clear();

        try
        {
            int written = 0;

            while ( byteBuffer.hasRemaining() ) {
                int writtenThisTime = getFileChannel().write( byteBuffer, position * recordSize + written );

                if (writtenThisTime == 0)
                    throw new IOException( "Unable to write to disk, reported bytes written was 0" );

                written += writtenThisTime;
            }
        }
        catch ( IOException e )
        {
            throw new UnderlyingStorageException( "Unable to write record["
                + position + "] @[" + position * recordSize + "]", e );
        }
    }
    
    @Override
    public int size()
    {
        return windowSize;
    }

    @Override
    public void force()
    {
        if ( isDirty() )
        {
            writeContents();
            setClean();
        }
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !(o instanceof AbstractPersistenceWindow) )
        {
            return false;
        }
        return position() == ((AbstractPersistenceWindow) o).position();
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
        // close called after flush all so no need to write out here
        buffer.close();
        closed = true;
    }
    
    @Override
    void acceptContents( PersistenceRow dpw )
    {
        ByteBuffer sourceBuffer = dpw.getBuffer().getBuffer();
        ByteBuffer targetBuffer = getBuffer().getBuffer();
        
        // The position of the row is the record to accept,
        // whereas the position of this window is the first record
        // in this window.
        targetBuffer.position( (int) ((dpw.position() - position()) * getRecordSize()) );
        sourceBuffer.clear();
        targetBuffer.put( sourceBuffer );
    }
}
