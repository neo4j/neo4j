/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.impl.nioneo.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

abstract class AbstractPersistenceWindow extends LockableWindow
{
    private final int recordSize;
    private final long position;
    private Buffer buffer = null;
    private final int totalSize;
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
        this.totalSize = totalSize;
        this.windowSize = totalSize / recordSize;
        this.buffer = new Buffer( this );
        this.buffer.setByteBuffer( byteBuffer );
    }

    public Buffer getBuffer()
    {
        return buffer;
    }
    
    public int getRecordSize()
    {
        return recordSize;
    }

    public Buffer getOffsettedBuffer( int id )
    {
        int offset = (int) ((id & 0xFFFFFFFFL) - 
            buffer.position()) * recordSize;
        buffer.setOffset( offset );
        return buffer;
    }
    
    public long position()
    {
        return position;
    }

    void readPosition()
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
            throw new StoreFailureException( "Unable to load position["
                + position + "] @[" + position * recordSize + "]", e );
        }
    }
    
    void writeOut()
    {
        ByteBuffer byteBuffer = buffer.getBuffer();
        byteBuffer.clear();
        try
        {
            int count = getFileChannel().write( byteBuffer,
                position * recordSize );
            assert count == totalSize;
        }
        catch ( IOException e )
        {
            throw new StoreFailureException( "Unable to write record["
                + position + "] @[" + position * recordSize + "]", e );
        }
    }

    public int size()
    {
        return windowSize;
    }

    public void force()
    {
        writeOut();
    }

    public boolean equals( Object o )
    {
        if ( !(o instanceof AbstractPersistenceWindow) )
        {
            return false;
        }
        return position() == ((AbstractPersistenceWindow) o).position();
    }

    public int hashCode()
    {
        return (int) this.position;
    }

    public String toString()
    {
        return "PersistenceRow[" + position + "]";
    }

    public void close()
    {
        // close called after flush all so no need to write out here
        buffer.close();
    }
}