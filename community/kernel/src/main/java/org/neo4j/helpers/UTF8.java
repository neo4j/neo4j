/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

/**
 * Utility class for converting strings to an from UTF-8 encoded bytes.
 *
 * @author Tobias Ivarsson <tobias.ivarsson@neotechnology.com>
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
            throw new Error( "UTF-8 should be available on all JVMs", e );
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
            throw new Error( "UTF-8 should be available on all JVMs", e );
        }
    }

    private UTF8()
    {
        // No need to instantiate, all methods are static
    }
}
