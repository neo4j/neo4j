/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration.helpers;

import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.CharUtils.isAsciiAlphaLower;

public class DatabaseNameValidator
{
    private static final int MINIMUM_DATABASE_NAME_LENGTH = 3;
    public static final int MAXIMUM_DATABASE_NAME_LENGTH = 63;
    public static final String DESCRIPTION = "Containing only alphabetic characters, numbers, dots and dashes, " +
            "with a length between " + MINIMUM_DATABASE_NAME_LENGTH + " and " + MAXIMUM_DATABASE_NAME_LENGTH + " characters. " +
            "It should be starting with an alphabetic character but not with the name 'system'.";
    private static final Pattern DATABASE_NAME_PATTERN = Pattern.compile( "^[a-z0-9-.]+$" );

    public static void validateExternalDatabaseName( NormalizedDatabaseName normalizedName )
    {
        validateInternalDatabaseName( normalizedName );

        var name = normalizedName.name();
        if ( name.startsWith( "system" ) )
        {
            throw new IllegalArgumentException( "Database name '" + name + "' is invalid, due to the prefix 'system'." );
        }
    }

    public static void validateInternalDatabaseName( NormalizedDatabaseName normalizedName )
    {
        Objects.requireNonNull( normalizedName, "The provided database name is empty." );

        String name = normalizedName.name();

        if ( name.isEmpty() )
        {
            throw new IllegalArgumentException( "The provided database name is empty." );
        }

        if ( name.length() < MINIMUM_DATABASE_NAME_LENGTH || name.length() > MAXIMUM_DATABASE_NAME_LENGTH )
        {
            throw new IllegalArgumentException( "The provided database name must have a length between " + MINIMUM_DATABASE_NAME_LENGTH +
                    " and " + MAXIMUM_DATABASE_NAME_LENGTH + " characters." );
        }

        if ( !isAsciiAlphaLower( name.charAt( 0 ) ) )
        {
            throw new IllegalArgumentException( "Database name '" + name + "' is not starting with an ASCII alphabetic character." );
        }

        if ( !DATABASE_NAME_PATTERN.matcher( name ).matches() )
        {
            throw new IllegalArgumentException(
                    "Database name '" + name + "' contains illegal characters. Use simple ascii characters, numbers, dots and dashes." );
        }
    }

    private static final Pattern DATABASE_NAME_GLOBBING_PATTERN = Pattern.compile( "^[a-z0-9-.*?]+$" );

    public static String validateDatabaseNamePattern( String name )
    {
        Objects.requireNonNull( name, "The provided database name is empty." );

        if ( name.trim().isEmpty() )
        {
            throw new IllegalArgumentException( "The provided database name is empty." );
        }
        name = name.toLowerCase();
        if ( name.length() > MAXIMUM_DATABASE_NAME_LENGTH )
        {
            throw new IllegalArgumentException( "The provided database name must have a length between " + 1 +
                                                " and " + MAXIMUM_DATABASE_NAME_LENGTH + " characters." );
        }

        if ( !DATABASE_NAME_GLOBBING_PATTERN.matcher( name ).matches() )
        {
            throw new IllegalArgumentException(
                    "Database name '" + name + "' contains illegal characters. Use simple ascii characters, numbers, dots," +
                    " question marks, asterisk and dashes." );
        }

        return name;
    }
}
