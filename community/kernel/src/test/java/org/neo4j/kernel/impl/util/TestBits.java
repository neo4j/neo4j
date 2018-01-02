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
package org.neo4j.kernel.impl.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import static org.neo4j.kernel.impl.util.Bits.bits;

public class TestBits
{
    @Test
    public void asBytes() throws Exception
    {
        int numberOfBytes = 14;
        Bits bits = bits( numberOfBytes );
        for ( byte i = 0; i < numberOfBytes; i++ )
        {
            bits.put( i );
        }

        byte[] bytes = bits.asBytes();
        for ( byte i = 0; i < numberOfBytes; i++ )
        {
            assertEquals( i, bytes[i] );
        }
    }

    @Test
    public void doubleAsBytes() throws Exception
    {
        double[] array1 = new double[] { 1.0, 2.0, 3.0, 4.0, 5.0 };
        Bits bits = Bits.bits( array1.length*8 );
        for ( double value : array1 )
        {
            bits.put( Double.doubleToRawLongBits( value ) );
        }
        String first = bits.toString();
        byte[] asBytes = bits.asBytes();
        String other = Bits.bitsFromBytes( asBytes ).toString();
        assertEquals( first, other );
    }

    @Test
    public void writeAndRead() throws Exception
    {
        for ( int b = 5; b <= 8; b++ )
        {
            Bits bits = Bits.bits( 16 );
            for ( byte value = 0; value < 16; value++ )
            {
                bits.put( value, b );
            }
            for ( byte expected = 0; bits.available(); expected++ )
            {
                assertEquals( expected, bits.getByte( b ) );
            }
        }

        for ( byte value = Byte.MIN_VALUE; value < Byte.MAX_VALUE; value++ )
        {
            Bits bits = Bits.bits( 8 );
            bits.put( value );
            assertEquals( value, bits.getByte() );
        }
    }

    @Test
    public void writeAndReadByteBuffer()
    {
        byte[] bytes = new byte[512];
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order( ByteOrder.LITTLE_ENDIAN );
        buffer.putLong( 123456789L );
        buffer.flip();
        Bits bits = Bits.bitsFromBytes( bytes, 0, buffer.limit() );

        assertEquals( 123456789L, bits.getLong());

    }
}
