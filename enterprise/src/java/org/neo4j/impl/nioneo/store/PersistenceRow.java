/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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

/**
 * PersistenRow only encapsulates one record in a store. It is used as a light-
 * weight window when no other (larger window) is found that encapsulates the
 * required record/block and it would be non efficient to create a large new
 * window to perform the required operation.
 */
class PersistenceRow extends LockableWindow
{
    private int recordSize = -1;
    private final long position;
    private Buffer buffer = null;

    PersistenceRow( long position, int recordSize, FileChannel channel )
    {
        super( channel );
        assert position >= 0 : "Illegal position[" + position + "]";
        assert recordSize > 0 : "Illegal recordSize[" + recordSize + "]";
        assert channel != null : "Null file channel";

        this.position = position;
        this.recordSize = recordSize;
        this.buffer = new Buffer( this );
        this.buffer.setByteBuffer( ByteBuffer.allocate( recordSize ) );
    }

    public Buffer getBuffer()
    {
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
            int recordCount = (int) (fileSize / recordSize);
            // possible last element not written completely, therefore if
            // fileSize % recordSize can be non 0 and we check > instead of >=
            if ( position > recordCount )
            {
                // use new buffer since it will contain only zeros
                return;
            }
            ByteBuffer byteBuffer = buffer.getBuffer();
            byteBuffer.clear();
            int count = getFileChannel().read( byteBuffer,
                position * recordSize );
            if ( position < recordCount )
            {
                assert count == recordSize;
            }
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
        if ( getOperationType() == OperationType.WRITE )
        {
            ByteBuffer byteBuffer = buffer.getBuffer();
            byteBuffer.clear();
            try
            {
                int count = getFileChannel().write( byteBuffer,
                    position * recordSize );
                assert count == recordSize;
            }
            catch ( IOException e )
            {
                throw new StoreFailureException( "Unable to write record["
                    + position + "] @[" + position * recordSize + "]", e );
            }
        }
    }

    public int size()
    {
        return 1;
    }

    public void force()
    {
        writeOut();
    }

    public boolean equals( Object o )
    {
        if ( !(o instanceof PersistenceRow) )
        {
            return false;
        }
        return position() == ((PersistenceRow) o).position();
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
        buffer.close();
    }
}