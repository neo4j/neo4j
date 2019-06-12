/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.kernel.database;

public class DatabaseNameValidator
{
    public static void assertValidDatabaseName( NormalizedDatabaseName normalizedName )
    {
        if ( normalizedName == null )
        {
            throw new IllegalArgumentException( "The provided database name is empty." );
        }

        String name = normalizedName.name();

        if ( name.length() == 0 )
        {
            throw new IllegalArgumentException( "The provided database name is empty." );
        }
        if ( name.length() < 3 || name.length() > 63 )
        {
            throw new IllegalArgumentException( "The provided database name must have a length between 3 and 63 characters." );
        }
        if ( !isLowerCaseLetter( (int) name.charAt( 0 ) ) )
        {
            throw new IllegalArgumentException( "Database name '" + name + "' is not starting with an ASCII alphabetic character." );
        }

        for ( char c : name.toCharArray() )
        {
            if ( !(isLowerCaseLetter( (int) c ) || isDigitDotOrDash( (int) c )) )
            {
                throw new IllegalArgumentException(
                        "Database name '" + name + "' contains illegal characters. Use simple ascii characters, numbers, dots and dashes." );
            }
        }

        if ( name.startsWith( "system" ) )
        {
            throw new IllegalArgumentException( "Database name '" + name + "' is invalid, due to the prefix 'system'." );
        }
    }

    private static boolean isLowerCaseLetter( int asciiCode )
    {
        return asciiCode >= 97 && asciiCode <= 122;
    }

    private static boolean isDigitDotOrDash( int asciiCode )
    {
        return (asciiCode >= 48 && asciiCode <= 57) || asciiCode == 46 || asciiCode == 45;
    }
}
