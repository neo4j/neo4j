/**
 * Copyright (c) 2002-2010 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.server.extensions;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

@Description( "An extension for accessing the reference node of the graph database, this can be used as the root for your graph." )
public class ReferenceNode extends ServerPlugin
{
    public static final String GET_REFERENCE_NODE = "reference_node_uri";

    @Description( "Get the reference node from the graph database" )
    @ExtensionTarget( GraphDatabaseService.class )
    @Name( GET_REFERENCE_NODE )
    public Node getReferenceNode( @Source GraphDatabaseService graphDb )
    {
        return graphDb.getReferenceNode();
    }
}
