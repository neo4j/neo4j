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
package org.neo4j.values.storable;

/**
 * Static methods for comparing and hashing chars, Strings and Text values.
 */
@SuppressWarnings( "WeakerAccess" )
public final class TextValues
{
    private TextValues()
    {
    }

    public static int compareCharToString( char c, String s )
    {
        int length = s.length();
        int x = length == 0 ? 1 : 0;
        if ( x == 0 )
        {
            x = Character.compare( c, s.charAt( 0 ) );
            if ( x == 0 && length > 1 )
            {
                x = -1;
            }
        }
        return x;
    }

    public static int compareTextArrays( TextArray a, TextArray b )
    {
        int i = 0;
        int x = 0;
        int length = Math.min( a.length(), b.length() );

        while ( x == 0 && i < length )
        {
            x = a.stringValue( i ).compareTo( b.stringValue( i ) );
            i++;
        }
        if ( x == 0 )
        {
            x = a.length() - b.length();
        }
        return x;
    }
}
