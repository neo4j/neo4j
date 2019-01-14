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
package org.neo4j.commandline.arguments;

import java.util.Collection;

import org.neo4j.helpers.Args;

public interface NamedArgument
{
    /**
     * Represents the option in the options list.
     */
    String optionsListing();

    /**
     * Represents the option in the usage string.
     */
    String usage();

    /**
     * An explanation of the option in the options list.
     */
    String description();

    /**
     * Name of the option as in '--name=<value>'
     */
    String name();

    /**
     * Example value listed in usage between brackets like '--name=<example-value>'
     */
    String exampleValue();

    /**
     * Parses the option (or possible default value) out of program arguments. Use only if a single argument is allowed.
     */
    String parse( Args parsedArgs );

    /**
     * Parses the option (or possible default value) out of program arguments. Use in case multiple arguments are
     * allowed.
     */
    Collection<String> parseMultiple( Args parsedArgs );

    /**
     * Returns true if this argument was given explicitly on the command line
     */
    default boolean has( Args parsedArgs )
    {
        return parsedArgs.has( name() );
    }
}
