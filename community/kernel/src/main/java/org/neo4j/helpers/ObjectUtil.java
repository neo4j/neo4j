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

import java.lang.reflect.Array;

/**
 * @deprecated This class will be removed in the next major release.
 */
@Deprecated
public class ObjectUtil
{
    public static String toString( Object obj )
    {
        if ( obj == null )
        {
            return "null";
        }
        else if ( obj.getClass().isArray() )
        {
            return arrayToString( obj );
        }
        else
        {
            return obj.toString();
        }
    }

    static String arrayToString( Object array )
    {
        StringBuilder result = new StringBuilder().append( '[' );
        for ( int i = 0, length = Array.getLength( array ); i < length; i++ )
        {
            if ( i != 0 )
            {
                result.append( ", " );
            }
            result.append( toString( Array.get( array, i ) ) );
        }
        return result.append( ']' ).toString();
    }
}
