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

import org.apache.commons.lang3.StringUtils;

import java.util.Optional;
import java.util.regex.Pattern;

import static org.neo4j.configuration.helpers.DatabaseNameValidator.validateDatabaseNamePattern;

public class DatabaseNamePattern
{
    private final Optional<Pattern> regexPattern;
    private final String databaseName;

    public DatabaseNamePattern( String name )
    {
        validateDatabaseNamePattern( name );
        this.regexPattern = buildRegexPattern( name.toLowerCase() );
        this.databaseName = name;
    }

    public boolean matches( String value )
    {
        return regexPattern.map( p -> p.matcher( value.toLowerCase() ).matches() )
                           .orElse( databaseName.equals( value ) );
    }

    public boolean containsPattern()
    {
        return regexPattern.isPresent();
    }

    public String getDatabaseName()
    {
        return databaseName;
    }

    public static Optional<Pattern> buildRegexPattern( String name )
    {
        if ( !StringUtils.containsAny( name, "*?" ) )
        {
            return Optional.empty();
        }

        final var pattern = new StringBuilder();
        for ( int i = 0; i < name.length(); i++ )
        {
            final var ch = name.charAt( i );
            if ( ch == '*' )
            {
                pattern.append( ".+" );
            }
            else if ( ch == '?' )
            {
                pattern.append( ".{0,1}" );
            }
            else if ( ch == '.' || ch == '-' )
            {
                pattern.append( "\\" ).append( ch );
            }
            else
            {
                pattern.append( ch );
            }
        }
        return Optional.of( Pattern.compile( pattern.toString() ) );
    }

    @Override
    public String toString()
    {
        if ( containsPattern() )
        {
            return "Database name pattern=" + databaseName;
        }
        else
        {
            return "Database name=" + databaseName;
        }
    }
}
