/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.plugins;

import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;

@Description( "Clones a subgraph (an example taken from a community mailing list requirement)" )
public class GraphCloner extends ServerPlugin
{

    public GraphCloner()
    {
        super( "GraphCloner" );
    }

    @PluginTarget( Node.class )
    public Node clonedSubgraph( @Source Node startNode, @Parameter( name = "depth", optional = false ) Integer depth )
    {
        GraphDatabaseService graphDb = startNode.getGraphDatabase();
        Transaction tx = graphDb.beginTx();
        try
        {
            Traverser traverse = traverseToDepth( startNode, depth );
            Iterator<Node> nodes = traverse.nodes()
                    .iterator();

            HashMap<Node, Node> clonedNodes = cloneNodes( graphDb, nodes );

            for ( Node oldNode : clonedNodes.keySet() )
            {
                // give me the matching new node
                Node newStartNode = clonedNodes.get( oldNode );

                // Now let's go through the relationships and copy them over
                Iterator<Relationship> oldRelationships = oldNode.getRelationships( Direction.OUTGOING )
                        .iterator();
                while ( oldRelationships.hasNext() )
                {
                    Relationship oldRelationship = oldRelationships.next();

                    Node newEndNode = clonedNodes.get( oldRelationship.getEndNode() );
                    if ( newEndNode != null )
                    {
                        Relationship newRelationship = newStartNode.createRelationshipTo( newEndNode,
                                oldRelationship.getType() );

                        cloneProperties( oldRelationship, newRelationship );
                    }
                }
            }

            tx.success();

            return clonedNodes.get( startNode );

        }
        finally
        {
            tx.finish();
        }
    }

    private void cloneProperties( Relationship oldRelationship, Relationship newRelationship )
    {
        Iterator<String> keys = oldRelationship.getPropertyKeys()
                .iterator();

        while ( keys.hasNext() )
        {
            String key = keys.next();
            newRelationship.setProperty( key, oldRelationship.getProperty( key ) );
        }
    }

    private Traverser traverseToDepth( final Node startNode, final int depth )
    {

        TraversalDescription traversalDescription = Traversal.description()
                .expand( PathExpanders.allTypesAndDirections() )
                .depthFirst()
                .evaluator( new Evaluator()
                {

                    @Override
                    public Evaluation evaluate( Path path )
                    {
                        if ( path.length() < depth )
                        {
                            return Evaluation.INCLUDE_AND_CONTINUE;
                        }
                        else
                        {
                            return Evaluation.INCLUDE_AND_PRUNE;
                        }
                    }
                } );

        return traversalDescription.traverse( startNode );

    }

    private Node cloneNodeData( GraphDatabaseService graphDb, Node node )
    {
        Node newNode = graphDb.createNode();
        for ( String key : node.getPropertyKeys() )
        {
            newNode.setProperty( key, node.getProperty( key ) );
        }
        return newNode;
    }

    private HashMap<Node, Node> cloneNodes( GraphDatabaseService graphDb, Iterator<Node> nodes )
    {
        HashMap<Node, Node> result = new HashMap<Node, Node>();

        while ( nodes.hasNext() )
        {
            Node next = nodes.next();
            result.put( next, cloneNodeData( graphDb, next ) );
        }

        return result;
    }
}
