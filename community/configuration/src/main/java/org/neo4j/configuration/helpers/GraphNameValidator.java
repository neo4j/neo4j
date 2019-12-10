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
package org.neo4j.configuration.helpers;

import java.util.Objects;
import java.util.regex.Pattern;

import static org.apache.commons.lang3.CharUtils.isAsciiAlphaLower;

public class GraphNameValidator
{
    public static final int MINIMUM_GRAPH_NAME_LENGTH = 3;
    public static final int MAXIMUM_GRAPH_NAME_LENGTH = 63;
    public static final String DESCRIPTION = "Containing only alphabetic characters, numbers, dots and dashes, " +
                                             "with a length between 3 and 63 characters. " +
                                             "It should be starting with an alphabetic character. The name 'graph' is reserved.";
    private static final Pattern NAME_PATTERN = Pattern.compile( "^[a-z0-9-.]+$" );

    public static void assertValidGraphName( NormalizedGraphName normalizedName )
    {
        Objects.requireNonNull( normalizedName, "The provided graph name is empty." );

        String name = normalizedName.name();

        if ( name.isEmpty() )
        {
            throw new IllegalArgumentException( "The provided graph name is empty." );
        }

        if ( name.length() < MINIMUM_GRAPH_NAME_LENGTH || name.length() > MAXIMUM_GRAPH_NAME_LENGTH )
        {
            throw new IllegalArgumentException( "The provided graph name must have a length between 3 and 63 characters." );
        }

        if ( !isAsciiAlphaLower( name.charAt( 0 ) ) )
        {
            throw new IllegalArgumentException( "Graph name '" + name + "' is not starting with an ASCII alphabetic character." );
        }

        if ( !NAME_PATTERN.matcher( name ).matches() )
        {
            throw new IllegalArgumentException(
                    "Graph name '" + name + "' contains illegal characters. Use simple ascii characters, numbers, dots and dashes." );
        }

        if ( "graph".equals( name ) )
        {
            throw new IllegalArgumentException( "Graph name 'graph' is reserved." );
        }
    }
}
