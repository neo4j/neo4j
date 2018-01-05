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

import static java.lang.Long.max;

/**
 * Stores {@link String strings} in a {@link ByteArray} provided by {@link NumberArrayFactory}. Each string can have different
 * length, where maximum string length is 2^16 - 1.
 */
public class StringCollisionValues implements CollisionValues
{
    private final ByteArray cache;
    private long offset;
    private ByteArray current;

    public StringCollisionValues( NumberArrayFactory factory, long length )
    {
        cache = factory.newDynamicByteArray( max( length, 10_000 ), new byte[1] );
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

        long endOffset = offset + 2 + bytes.length;
        ByteArray end = cache.at( endOffset );
        if ( end != current )
        {
            offset = endOffset;
            current = end;
        }

        long startOffset = offset;
        current.setByte( offset++, 0, (byte) (length & 0xFF) );
        current.setByte( offset++, 0, (byte) ((length >>> 8) & 0xFF) );
        for ( byte charByte : bytes )
        {
            current.setByte( offset++, 0, charByte );
        }

        return startOffset;
    }

    @Override
    public Object get( long offset )
    {
        ByteArray array = cache.at( offset );
        int lengthLsb = array.getByte( offset++, 0 ) & 0xFF;
        int lengthMsb = array.getByte( offset++, 0 ) & 0xFF;
        int length = (lengthMsb << 8) | lengthLsb;
        byte[] bytes = new byte[length];
        for ( int i = 0; i < length; i++ )
        {
            bytes[i] = array.getByte( offset++, 0 );
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
