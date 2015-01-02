/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

/**
 * Helper functions for working with strings.
 */
public final class Strings
{
    private Strings()
    {
    }

    public static final Function<String, String> decamelize = new Function<String, String>()
    {
        @Override
        public String apply( String name )
        {
            StringBuilder result = new StringBuilder();
            for ( int i = 0; i < name.length(); i++ )
            {
                char c = name.charAt( i );
                if ( Character.isUpperCase( c ) )
                {
                    if ( i > 0 )
                    {
                        result.append( '_' );
                    }
                    result.append( Character.toLowerCase( c ) );
                }
                else
                {
                    result.append( c );
                }
            }
            return result.toString();
        }
    };
}
