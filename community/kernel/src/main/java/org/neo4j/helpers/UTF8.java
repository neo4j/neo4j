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
package org.neo4j.helpers;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

/**
 * Utility class for converting strings to and from UTF-8 encoded bytes.
 */
public final class UTF8
{
    public static final Function<String, byte[]> encode = new Function<String, byte[]>()
    {
        @Override
        public byte[] apply( String s )
        {
            return encode( s );
        }
    };

    public static final Function<byte[], String> decode = new Function<byte[], String>()
    {
        @Override
        public String apply( byte[] bytes )
        {
            return decode( bytes );
        }
    };

    public static byte[] encode( String string )
    {
        try
        {
            return string.getBytes( "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw cantBelieveUtf8IsntAvailableInThisJvmError( e );
        }
    }

    public static String decode( byte[] bytes )
    {
        try
        {
            return new String( bytes, "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw cantBelieveUtf8IsntAvailableInThisJvmError( e );
        }
    }

    public static String decode( byte[] bytes, int offset, int length )
    {
        try
        {
            return new String( bytes, offset, length, "UTF-8" );
        }
        catch ( UnsupportedEncodingException e )
        {
            throw cantBelieveUtf8IsntAvailableInThisJvmError( e );
        }
    }
    
    public static String getDecodedStringFrom( ByteBuffer source )
    {
        // Currently only one key is supported although the data format supports multiple
        int count = source.getInt();
        byte[] data = new byte[count];
        source.get( data );
        return UTF8.decode( data );
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

    private static Error cantBelieveUtf8IsntAvailableInThisJvmError( UnsupportedEncodingException e )
    {
        return new Error( "UTF-8 should be available on all JVMs", e );
    }
    
    private UTF8()
    {
        // No need to instantiate, all methods are static
    }

}
