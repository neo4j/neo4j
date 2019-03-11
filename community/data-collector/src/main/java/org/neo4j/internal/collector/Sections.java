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
package org.neo4j.internal.collector;

import java.util.Arrays;

import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;

class Sections
{
    private Sections()
    { // only static methods
    }

    static final String GRAPH_COUNTS = "GRAPH COUNTS";
    static final String TOKENS = "TOKENS";
    static final String META = "META";
    static final String QUERIES = "QUERIES";

    private static final String[] SECTIONS = {GRAPH_COUNTS, TOKENS, QUERIES};
    private static final String NAMES = Arrays.toString( SECTIONS );

    static InvalidArgumentsException unknownSectionException( String section )
    {
        return new InvalidArgumentsException( String.format( "Unknown section '%s', known sections are %s",
                                                             section, NAMES ) );
    }
}
