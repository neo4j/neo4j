/*
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.neo4j.examples.server.plugins;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.server.plugins.Description;
import org.neo4j.server.plugins.PluginTarget;
import org.neo4j.server.plugins.ServerPlugin;
import org.neo4j.server.plugins.Source;

/**
 * An extension performing a predefined graph traversal
 */
@Description( "Performs a depth two traversal along all relationship types." )
public class DepthTwo extends ServerPlugin
{
    @Description( "Traverse depth two and return the end nodes" )
    @PluginTarget( Node.class )
    public Iterable<Node> nodesOnDepthTwo( @Source Node node )
    {
        ArrayList<Node> nodes = new ArrayList<>();
        try (Transaction tx = node.getGraphDatabase().beginTx())
        {
            for ( Node foundNode : getTraversal( node ).traverse( node ).nodes() )
            {
                nodes.add( foundNode );
            }
            tx.success();
        }
        return nodes;
    }

    @Description( "Traverse depth two and return the last relationships" )
    @PluginTarget( Node.class )
    public Iterable<Relationship> relationshipsOnDepthTwo( @Source Node node )
    {
        List<Relationship> rels = new ArrayList<>();
        try (Transaction tx = node.getGraphDatabase().beginTx())
        {
            for ( Relationship rel : getTraversal( node ).traverse( node ).relationships() )
            {
                rels.add( rel );
            }
            tx.success();
        }
        return rels;
    }

    @Description( "Traverse depth two and return the paths" )
    @PluginTarget( Node.class )
    public Iterable<Path> pathsOnDepthTwo( @Source Node node )
    {
        List<Path> paths = new ArrayList<>();
        try (Transaction tx = node.getGraphDatabase().beginTx())
        {
            for ( Path path : getTraversal( node ).traverse( node ) )
            {
                paths.add( path );
            }
            tx.success();
        }
        return paths;
    }

    private TraversalDescription getTraversal( Node node )
    {
        return node.getGraphDatabase()
                .traversalDescription()
                .uniqueness( Uniqueness.RELATIONSHIP_PATH )
                .evaluator( Evaluators.atDepth( 2 ) );
    }
}
