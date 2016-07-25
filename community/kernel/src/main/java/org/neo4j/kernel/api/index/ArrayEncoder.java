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
package org.neo4j.kernel.api.index;

import java.lang.reflect.Array;
import java.util.Base64;

import org.neo4j.string.UTF8;

public final class ArrayEncoder
{
    private static final Base64.Encoder base64Encoder = Base64.getEncoder();

    private ArrayEncoder()
    {
        throw new AssertionError( "Not for instantiation!" );
    }

    public static String encode( Object array )
    {
        if ( !array.getClass().isArray() )
        {
            throw new IllegalArgumentException( "Only works with arrays" );
        }

        StringBuilder builder = new StringBuilder();
        int length = Array.getLength( array );
        String type = "";
        for ( int i = 0; i < length; i++ )
        {
            Object o = Array.get( array, i );
            if ( o instanceof Number )
            {
                type = "D";
                builder.append( ((Number) o).doubleValue() );
            }
            else if ( o instanceof Boolean )
            {
                type = "Z";
                builder.append( o );
            }
            else
            {
                type = "L";
                String str = o.toString();
                builder.append( base64Encoder.encodeToString( UTF8.encode( str ) ) );
            }
            builder.append( "|" );
        }
        return type + builder.toString();
    }
}
