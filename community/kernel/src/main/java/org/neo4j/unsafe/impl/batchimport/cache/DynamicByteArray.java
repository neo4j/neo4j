/*
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.nio.ByteBuffer;

import static org.neo4j.unsafe.impl.batchimport.cache.HeapByteArray.get6BLongFromByteBuffer;

public class DynamicByteArray extends DynamicNumberArray<ByteArray> implements ByteArray
{
    private final byte[] defaultValue;
    private final ByteBuffer defaultValueConvenienceBuffer;

    public DynamicByteArray( NumberArrayFactory factory, long chunkSize, byte[] defaultValue )
    {
        super( factory, chunkSize, new ByteArray[0] );
        this.defaultValue = defaultValue;
        this.defaultValueConvenienceBuffer = ByteBuffer.wrap( defaultValue );
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        // Let's just do this the stupid way. There's room for optimization here
        byte[] intermediary = defaultValue.clone();
        byte[] transport = defaultValue.clone();
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            get( fromIndex+i, intermediary );
            get( toIndex+i, transport );
            set( fromIndex+i, transport );
            set( toIndex+i, intermediary );
        }
    }

    @Override
    public void get( long index, byte[] into )
    {
        ByteArray chunk = chunkOrNullAt( index );
        if ( chunk != null )
        {
            chunk.get( index, into );
        }
        else
        {
            System.arraycopy( defaultValue, 0, into, 0, defaultValue.length );
        }
    }

    @Override
    public byte getByte( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.getByte( index, offset ) : defaultValueConvenienceBuffer.get( offset );
    }

    @Override
    public short getShort( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.getShort( index, offset ) : defaultValueConvenienceBuffer.getShort( offset );
    }

    @Override
    public int getInt( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.getInt( index, offset ) : defaultValueConvenienceBuffer.getInt( offset );
    }

    @Override
    public long get6ByteLong( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.get6ByteLong( index, offset ) :
            get6BLongFromByteBuffer( defaultValueConvenienceBuffer, offset );
    }

    @Override
    public long getLong( long index, int offset )
    {
        ByteArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.getLong( index, offset ) : defaultValueConvenienceBuffer.getLong( offset );
    }

    @Override
    public void set( long index, byte[] value )
    {
        at( index ).set( index, value );
    }

    @Override
    public void setByte( long index, int offset, byte value )
    {
        at( index ).setByte( index, offset, value );
    }

    @Override
    public void setShort( long index, int offset, short value )
    {
        at( index ).setShort( index, offset, value );
    }

    @Override
    public void setInt( long index, int offset, int value )
    {
        at( index ).setInt( index, offset, value );
    }

    @Override
    public void set6ByteLong( long index, int offset, long value )
    {
        at( index ).set6ByteLong( index, offset, value );
    }

    @Override
    public void setLong( long index, int offset, long value )
    {
        at( index ).setLong( index, offset, value );
    }

    @Override
    protected ByteArray addChunk( long chunkSize, long base )
    {
        return factory.newByteArray( chunkSize, defaultValue, base );
    }
}
