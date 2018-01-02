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
package org.neo4j.shell.kernel.apps.cypher;

import org.neo4j.helpers.Service;
import org.neo4j.shell.App;

@Service.Implementation( App.class )
public class Cypher extends Start
{
    @Override
    public String getDescription()
    {
        return "Executes a Cypher query with an older parser. " +
                "Usage: cypher <version> start <rest of query>\n" +
                "Example: CYPHER 1.5 START me = node({self}) MATCH me-[:KNOWS]->you RETURN you.name\n" +
                "where {self} will be replaced with the current location in the graph";
    }
}
