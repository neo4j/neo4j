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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ByteArrayTest
{
    @Test
    public void shouldSetAndGetBasicTypes() throws Exception
    {
        // GIVEN
        ByteArray array = newArray( 100, new byte[15] );

        // WHEN
        array.setByte( 0, 0, (byte) 123 );
        array.setShort( 0, 1, (short) 1234 );
        array.setInt( 0, 5, 12345 );
        array.setLong( 0, 9, Long.MAX_VALUE - 100 );

        // THEN
        assertEquals( (byte) 123, array.getByte( 0, 0 ) );
        assertEquals( (short) 1234, array.getShort( 0, 1 ) );
        assertEquals( 12345, array.getInt( 0, 5 ) );
        assertEquals( Long.MAX_VALUE - 100, array.getLong( 0, 9 ) );
    }

    @Test
    public void shouldDetectMinusOne() throws Exception
    {
        // GIVEN
        ByteArray array = newArray( 100, new byte[15] );

        // WHEN
        array.set6BLong( 10, 2, -1 );
        array.set6BLong( 10, 8, -1 );

        // THEN
        assertEquals( -1L, array.get6BLong( 10, 2 ) );
        assertEquals( -1L, array.get6BLong( 10, 8 ) );
    }

    private ByteArray newArray( int length, byte[] defaultValue )
    {
        return NumberArrayFactory.HEAP.newByteArray( length, defaultValue );
    }
}
