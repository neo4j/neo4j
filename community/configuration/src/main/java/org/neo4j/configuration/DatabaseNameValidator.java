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
package org.neo4j.configuration;

import java.util.Objects;
import java.util.regex.Pattern;

public class DatabaseNameValidator
{
    private static Pattern databaseNamePattern = Pattern.compile( "^[a-z0-9-.]+$" );
    private static Pattern startsWithLowerCaseLetterPattern = Pattern.compile( "^[a-z].*" );

    public static void assertValidDatabaseName( NormalizedDatabaseName normalizedName )
    {
        Objects.requireNonNull( normalizedName, "The provided database name is empty." );

        String name = normalizedName.name();

        if ( name.isEmpty() )
        {
            throw new IllegalArgumentException( "The provided database name is empty." );
        }

        if ( name.length() < 3 || name.length() > 63 )
        {
            throw new IllegalArgumentException( "The provided database name must have a length between 3 and 63 characters." );
        }

        if ( !startsWithLowerCaseLetterPattern.matcher(  name ).matches() )
        {
            throw new IllegalArgumentException( "Database name '" + name + "' is not starting with an ASCII alphabetic character." );
        }

        if ( !databaseNamePattern.matcher( name ).matches() )
        {
            throw new IllegalArgumentException(
                    "Database name '" + name + "' contains illegal characters. Use simple ascii characters, numbers, dots and dashes." );
        }

        if ( name.startsWith( "system" ) )
        {
            throw new IllegalArgumentException( "Database name '" + name + "' is invalid, due to the prefix 'system'." );
        }
    }
}
