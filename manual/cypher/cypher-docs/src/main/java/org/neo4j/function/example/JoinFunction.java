/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.function.example;

import java.util.List;

import org.neo4j.procedure.Description;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.UserFunction;

// START SNIPPET: joinFunction
public class JoinFunction
{

    /**
     * Joins a list of strings together using the provided delimiter
     *
     * @param strings the strings to join
     * @param delimiter the delimiter to join with (optional)
     * @return A string composed of the <code>strings</code> joined using the <code>delimiter</code>
     */
    @UserFunction
    @Description( "org.neo4j.function.example.join(['s1','s2',...], delimiter) - join the given strings with the " +
                  "given delimiter." )
    public String join(
            @Name( "strings" ) List<String> strings,
            @Name( value = "delimiter", defaultValue = "," ) String delimiter )
    {
        if ( strings == null || delimiter == null )
        {
            return null;
        }
        return String.join( delimiter, strings );
    }
}
// END SNIPPET: joinFunction
