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
package org.neo4j.io.pagecache.impl.common;

import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;

import org.neo4j.io.pagecache.PageCursor;

public abstract class OffsetTrackingCursor implements PageCursor
{
    protected Page page;
    private int currentOffset;

    public byte getByte()
    {
        try
        {
            byte value = page.getByte( currentOffset );
            currentOffset += 1;
            return value;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public void putByte( byte value )
    {
        try
        {
            page.putByte( value, currentOffset );
            currentOffset += 1;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public long getLong()
    {
        try
        {
            long value = page.getLong( currentOffset );
            currentOffset += 8;
            return value;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public void putLong( long l )
    {
        try
        {
            page.putLong( l, currentOffset );
            currentOffset += 8;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public int getInt()
    {
        try
        {
            int value = page.getInt( currentOffset );
            currentOffset += 4;
            return value;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public long getUnsignedInt()
    {
        return getInt() & 0xFFFFFFFFL;
    }

    public void putInt( int i )
    {
        try
        {
            page.putInt( i, currentOffset );
            currentOffset += 4;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public void getBytes( byte[] data )
    {
        try
        {
            page.getBytes( data, currentOffset );
            currentOffset += data.length;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public void putBytes( byte[] data )
    {
        try
        {
            page.putBytes( data, currentOffset );
            currentOffset += data.length;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public void setOffset( int offset )
    {
        currentOffset = offset;
    }

    @Override
    public short getShort()
    {
        try
        {
            short value = page.getShort( currentOffset );
            currentOffset += 2;
            return value;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    @Override
    public void putShort( short value )
    {
        try
        {
            page.putShort( value, currentOffset );
            currentOffset += 2;
        } catch(IndexOutOfBoundsException | BufferOverflowException | BufferUnderflowException e )
        {
            throw outOfBoundsException( e );
        }
    }

    public int getOffset()
    {
        return currentOffset;
    }

    public OffsetTrackingCursor reset( Page page )
    {
        this.page = page;
        currentOffset = 0;
        return this;
    }

    public Page getPage()
    {
        return page;
    }

    @Override
    public String toString()
    {
        return "OffsetTrackingCursor{" +
                "currentOffset=" + currentOffset +
                ", page=" + page +
                '}';
    }

    private RuntimeException outOfBoundsException( RuntimeException e )
    {
        return new RuntimeException( "Failed to read or write to page: " + toString(), e );
    }
}
