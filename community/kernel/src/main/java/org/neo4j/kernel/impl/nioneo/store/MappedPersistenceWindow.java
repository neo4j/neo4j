/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Persistence window using a {@link MappedByteBuffer} as underlying buffer.
 */
class MappedPersistenceWindow extends LockableWindow
{
    private long position = -1;
    private Buffer buffer = null;
    private int windowSize = -1;
    private int recordSize = -1;
    private int totalSize = -1;

    MappedPersistenceWindow( long position, int recordSize, int totalSize,
        StoreChannel channel, FileChannel.MapMode mapMode )
    {
        super( channel );
        assert recordSize > 0 : "Record size[" + recordSize
            + "] must be greater then zero";
        assert totalSize >= recordSize : "Total size[" + totalSize
            + "] cannot be less than record size[" + recordSize + "]";
        assert totalSize >= recordSize : "Total size[" + totalSize
            + "] must mod to zero with record size[" + recordSize + "]";
        this.totalSize = totalSize;
        windowSize = totalSize / recordSize;
        this.recordSize = recordSize;
        this.position = position;
        try
        {
            buffer = new Buffer( this, channel.map( mapMode,
                    position * recordSize, totalSize ) );
        }
        catch ( IOException e )
        {
            this.position = -1;
            throw new MappedMemException( "Unable to map pos=" + position + 
                    " recordSize=" + recordSize + " totalSize=" + totalSize, e );
        }
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
    public long position()
    {
        return position;
    }

    @Override
    public int size()
    {
        return windowSize;
    }

    @Override
    public void force()
    {
        ((MappedByteBuffer) buffer.getBuffer()).force();
    }

    @Override
    public boolean equals( Object o )
    {
        if ( !(o instanceof MappedPersistenceWindow) )
        {
            return false;
        }
        return position() == ((MappedPersistenceWindow) o).position();
    }

    private volatile int hashCode = 0;

    @Override
    public int hashCode()
    {
        if ( hashCode == 0 )
        {
            hashCode = (int) position();
        }
        return hashCode;
    }

    @Override
    public String toString()
    {
        return "MappedPersistenceWindow[p=" + position + ",rs=" + recordSize
            + ",ws=" + windowSize + ",ts=" + totalSize + "]";
    }

    @Override
    public synchronized void close()
    {
        buffer.close();
        position = -1;
        closed = true;
    }

    @Override
    public Buffer getOffsettedBuffer( long id )
    {
        int offset = (int) (id - position) * recordSize;
        try
        {
            buffer.setOffset( offset );
            return buffer;
        } catch(IllegalArgumentException e)
        {
            throw new IllegalArgumentException(
                    "Unable to set offset. id: " + id +
                            ", position: " + position + ", recordSize: " + recordSize, e );
        }
    }
}
