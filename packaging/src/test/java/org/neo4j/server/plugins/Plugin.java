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

package org.neo4j.server.plugins;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.util.ArrayList;
import java.util.Arrays;

@Description( "An extension for accessing the reference node of the graph database, this can be used as the root for your graph." )
public class Plugin extends ServerPlugin
{
    public static final String GET_REFERENCE_NODE = "reference_node_uri";
    public static final String GET_CONNECTED_NODES = "connected_nodes";

    @Description( "Get the reference node from the graph database" )
    @PluginTarget( GraphDatabaseService.class )
    @Name( GET_REFERENCE_NODE )
    public Node getReferenceNode( @Source GraphDatabaseService graphDb )
    {
        return graphDb.getReferenceNode();
    }

    @Name( GET_CONNECTED_NODES )
    @PluginTarget( Node.class )
    public Iterable<Node> getAllConnectedNodes( @Source Node start )
    {
        ArrayList<Node> nodes = new ArrayList<Node>();

        for ( Relationship rel : start.getRelationships() )
        {
            nodes.add( rel.getOtherNode( start ) );
        }

        return nodes;
    }

    @PluginTarget( GraphDatabaseService.class )
    public Node methodWithIntParam(
            @Source GraphDatabaseService db,
            @Parameter( name = "id", optional = false ) Integer id )
    {
        return db.getNodeById( id );
    }

    @PluginTarget( Relationship.class )
    public Iterable<Node> methodOnRelationship( @Source Relationship rel )
    {
        return Arrays.asList( rel.getNodes() );
    }


}
