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
package org.neo4j.kernel.impl.store.kvstore;

import org.hamcrest.Matcher;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class BigEndianByteArrayBufferTest
{
    BigEndianByteArrayBuffer buffer = new BigEndianByteArrayBuffer( new byte[8] );

    @Test
    public void shouldWriteLong() throws Exception
    {
        // when
        buffer.putLong( 0, 0xABCDEF0123456789L );

        // then
        assertEquals( 0xAB, 0xFF & buffer.getByte( 0 ) );
        assertEquals( 0xCD, 0xFF & buffer.getByte( 1 ) );
        assertEquals( 0xEF, 0xFF & buffer.getByte( 2 ) );
        assertEquals( 0x01, 0xFF & buffer.getByte( 3 ) );
        assertEquals( 0x23, 0xFF & buffer.getByte( 4 ) );
        assertEquals( 0x45, 0xFF & buffer.getByte( 5 ) );
        assertEquals( 0x67, 0xFF & buffer.getByte( 6 ) );
        assertEquals( 0x89, 0xFF & buffer.getByte( 7 ) );
        assertEquals( 0xABCDEF0123456789L, buffer.getLong( 0 ) );
    }

    @Test
    public void shouldWriteInt() throws Exception
    {
        // when
        buffer.putInt( 0, 0x12345678 );
        buffer.putInt( 4, 0x87654321 );

        // then
        assertEquals( 0x12345678, buffer.getInt( 0 ) );
        assertEquals( 0x87654321, buffer.getInt( 4 ) );
        assertEquals( 0x1234567887654321L, buffer.getLong( 0 ) );
    }

    @Test
    public void shouldWriteShort() throws Exception
    {
        // when
        buffer.putShort( 0, (short) 0x1234 );
        buffer.putShort( 2, (short) 0x4321 );
        buffer.putShort( 4, (short) 0xABCD );
        buffer.putShort( 6, (short) 0xFEDC );

        // then
        assertEquals( (short) 0x1234, buffer.getShort( 0 ) );
        assertEquals( (short) 0x4321, buffer.getShort( 2 ) );
        assertEquals( (short) 0xABCD, buffer.getShort( 4 ) );
        assertEquals( (short) 0xFEDC, buffer.getShort( 6 ) );
    }

    @Test
    public void shouldWriteChar() throws Exception
    {
        // when
        buffer.putChar( 0, 'H' );
        buffer.putChar( 2, 'E' );
        buffer.putChar( 4, 'L' );
        buffer.putChar( 6, 'O' );

        // then
        assertEquals( 'H', buffer.getChar( 0 ) );
        assertEquals( 'E', buffer.getChar( 2 ) );
        assertEquals( 'L', buffer.getChar( 4 ) );
        assertEquals( 'O', buffer.getChar( 6 ) );
    }

    @Test
    public void shouldWriteByte() throws Exception
    {
        // when
        for ( int i = 0; i < buffer.size(); i++ )
        {
            buffer.putByte( i, (byte) ((1 << i) + i) );
        }

        // then
        for ( int i = 0; i < buffer.size(); i++ )
        {
            assertEquals( (byte) ((1 << i) + i), buffer.getByte( i ) );
        }
    }

    @Test
    public void shouldCompareByteArrays() throws Exception
    {
        // given
        Matcher<Integer> LESS_THAN = lessThan( 0 ), GREATER_THAN = greaterThan( 0 ), EQUAL_TO = equalTo( 0 );

        // then
        assertCompare( new byte[0], EQUAL_TO, new byte[0] );
        assertCompare( new byte[]{1, 2, 3}, EQUAL_TO, new byte[]{1, 2, 3} );
        assertCompare( new byte[]{1, 2, 3}, LESS_THAN, new byte[]{1, 2, 4} );
        assertCompare( new byte[]{1, 2, 3}, LESS_THAN, new byte[]{2, 2, 3} );
        assertCompare( new byte[]{1, 2, 3}, GREATER_THAN, new byte[]{1, 2, 0} );
        assertCompare( new byte[]{1, 2, 3}, GREATER_THAN, new byte[]{0, 2, 3} );
    }

    private static void assertCompare( byte[] lhs, Matcher<Integer> isAsExpected, byte[] rhs )
    {
        assertThat( BigEndianByteArrayBuffer.compare( lhs, rhs, 0 ), isAsExpected );
    }
}