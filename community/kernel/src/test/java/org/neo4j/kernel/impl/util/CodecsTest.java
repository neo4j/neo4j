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

import org.junit.Test;

import static org.junit.Assert.*;

public class CodecsTest
{
    @Test
    public void shouldEncodeBytesToString()
    {
        String result = Codecs.encodeHexString( new byte[]{(byte) 0xFF, (byte) 0x94, (byte) 0x5C, (byte) 0x00, (byte) 0x3D} );
        assertEquals( "FF945C003D", result );
    }

    @Test
    public void shouldEncodeEmptyBytesToEmptyString()
    {
        String result = Codecs.encodeHexString( new byte[]{} );
        assertEquals( "", result );
    }

    @Test
    public void shouldDecodeStringToBytes()
    {
        byte[] result = Codecs.decodeHexString( "00f34CEFFF3e02" );
        byte[] expected = new byte[] {(byte) 0x00, (byte) 0xF3, (byte) 0x4C, (byte) 0xEF, (byte) 0xFF, (byte) 0x3E, (byte) 0x02 };
        assertArrayEquals( expected, result );
    }

    @Test
    public void shouldDecodeEmptyStringToEmptyBytes()
    {
        byte[] result = Codecs.decodeHexString( "" );
        assertArrayEquals( new byte[]{}, result );
    }
}
