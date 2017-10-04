/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.string;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;

/**
 * Utilities for working with UTF8 encoding and decoding.
 */
public class UTF8
{
    public static final Function<String, byte[]> encode = UTF8::encode;

    public static final Function<byte[], String> decode = UTF8::decode;
    public static final int MINIMUM_SERIALISED_LENGTH_BYTES = Integer.BYTES;

    public static byte[] encode( String string )
    {
        return string.getBytes( StandardCharsets.UTF_8 );
    }

    public static String decode( byte[] bytes )
    {
        return new String( bytes, StandardCharsets.UTF_8 );
    }

    public static String decode( byte[] bytes, int offset, int length )
    {
        return new String( bytes, offset, length, StandardCharsets.UTF_8 );
    }

    public static String getDecodedStringFrom( ByteBuffer source )
    {
        // Currently only one key is supported although the data format supports multiple
        int count = source.getInt();
        int remaining = source.remaining();
        if ( count > remaining )
        {
            throw badStringFormatException( count, remaining );
        }
        byte[] data = new byte[count];
        source.get( data );
        return UTF8.decode( data );
    }

    private static IllegalArgumentException badStringFormatException( int count, int remaining )
    {
        return new IllegalArgumentException(
                "Bad string format; claims string is " + count + " bytes long, " +
                "but only " + remaining + " bytes remain in buffer" );
    }

    public static void putEncodedStringInto( String text, ByteBuffer target )
    {
        byte[] data = encode( text );
        target.putInt( data.length );
        target.put( data );
    }

    public static int computeRequiredByteBufferSize( String text )
    {
        return encode( text ).length + 4;
    }

    private UTF8()
    {
        // No need to instantiate, all methods are static
    }
}
