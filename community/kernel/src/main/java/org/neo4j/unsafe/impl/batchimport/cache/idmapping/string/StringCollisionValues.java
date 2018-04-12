/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import org.neo4j.string.UTF8;
import org.neo4j.unsafe.impl.batchimport.cache.ByteArray;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;

import static java.lang.Integer.min;
import static java.lang.Long.max;
import static java.lang.Long.min;

/**
 * Stores {@link String strings} in a {@link ByteArray} provided by {@link NumberArrayFactory}. Each string can have different
 * length, where maximum string length is 2^16 - 1.
 */
public class StringCollisionValues implements CollisionValues
{
    private final long chunkSize;
    private final ByteArray cache;
    private long offset;
    private ByteArray current;

    public StringCollisionValues( NumberArrayFactory factory, long length )
    {
        chunkSize = max( length, 10_000 );
        cache = factory.newDynamicByteArray( chunkSize, new byte[1] );
        current = cache.at( 0 );
    }

    @Override
    public long add( Object id )
    {
        String string = (String) id;
        byte[] bytes = UTF8.encode( string );
        int length = bytes.length;
        if ( length > 0xFFFF )
        {
            throw new IllegalArgumentException( string );
        }

        long bytesLeftInThisChunk = bytesLeftInCurrentChunk();
        if ( bytesLeftInThisChunk < Short.BYTES + 1 )
        {
            // There isn't enough space left in the current chunk to begin writing this value, move over to the next one
            offset += chunkSize - (offset % chunkSize);
            current = cache.at( offset );
        }

        long startOffset = offset;
        current.setShort( offset, 0, (short) length );
        offset += Short.BYTES;
        for ( int i = 0; i < length; )
        {
            int bytesLeftToWrite = length - i;
            int bytesLeftInChunk = (int) (chunkSize - offset % chunkSize);
            int bytesToWriteInThisChunk = min( bytesLeftToWrite, bytesLeftInChunk );
            for ( int j = 0; j < bytesToWriteInThisChunk; j++ )
            {
                current.setByte( offset++, 0, bytes[i++] );
            }

            if ( length > i )
            {
                current = cache.at( offset );
            }
        }

        return startOffset;
    }

    private long bytesLeftInCurrentChunk()
    {
        long rest = offset % chunkSize;
        return rest == 0 ? 0 : chunkSize - rest;
    }

    @Override
    public Object get( long offset )
    {
        ByteArray array = cache.at( offset );
        int length = array.getShort( offset, 0 ) & 0xFFFF;
        offset += Short.BYTES;
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; )
        {
            int bytesLeftToRead = length - i;
            int bytesLeftInChunk = (int) (chunkSize - offset % chunkSize);
            int bytesToReadInThisChunk = min( bytesLeftToRead, bytesLeftInChunk );
            for ( int j = 0; j < bytesToReadInThisChunk; j++ )
            {
                bytes[i++] = array.getByte( offset++, 0 );
            }

            if ( length > i )
            {
                array = cache.at( offset );
            }
        }
        return UTF8.decode( bytes );
    }

    @Override
    public void acceptMemoryStatsVisitor( MemoryStatsVisitor visitor )
    {
        cache.acceptMemoryStatsVisitor( visitor );
    }

    @Override
    public void close()
    {
        cache.close();
    }
}
