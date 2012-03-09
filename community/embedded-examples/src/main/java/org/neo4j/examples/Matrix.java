/**
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
package org.neo4j.examples;

import java.io.File;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;

public class Matrix
{
    public enum RelTypes implements RelationshipType
    {
        NEO_NODE,
        KNOWS,
        CODED_BY
    }

    private static final String MATRIX_DB = "target/matrix-db";
    private GraphDatabaseService graphDb;

    public static void main( String[] args )
    {
        Matrix matrix = new Matrix();
        matrix.setUp();
        System.out.println( matrix.printNeoFriends() );
        System.out.println( matrix.printMatrixHackers() );
        matrix.shutdown();
    }

    public void setUp()
    {
        deleteFileOrDirectory( new File( MATRIX_DB ) );
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( MATRIX_DB ).newGraphDatabase();
        registerShutdownHook();
        createNodespace();
    }

    public void shutdown()
    {
        graphDb.shutdown();
    }

    public void createNodespace()
    {
        Transaction tx = graphDb.beginTx();
        try
        {
            Node thomas = graphDb.createNode();
            thomas.setProperty( "name", "Thomas Anderson" );
            thomas.setProperty( "age", 29 );

            // connect Neo/Thomas to the reference node
            Node referenceNode = graphDb.getReferenceNode();
            referenceNode.createRelationshipTo( thomas, RelTypes.NEO_NODE );

            Node trinity = graphDb.createNode();
            trinity.setProperty( "name", "Trinity" );
            Relationship rel = thomas.createRelationshipTo( trinity,
                    RelTypes.KNOWS );
            rel.setProperty( "age", "3 days" );
            Node morpheus = graphDb.createNode();
            morpheus.setProperty( "name", "Morpheus" );
            morpheus.setProperty( "rank", "Captain" );
            morpheus.setProperty( "occupation", "Total badass" );
            thomas.createRelationshipTo( morpheus, RelTypes.KNOWS );
            rel = morpheus.createRelationshipTo( trinity, RelTypes.KNOWS );
            rel.setProperty( "age", "12 years" );
            Node cypher = graphDb.createNode();
            cypher.setProperty( "name", "Cypher" );
            cypher.setProperty( "last name", "Reagan" );
            trinity.createRelationshipTo( cypher, RelTypes.KNOWS );
            rel = morpheus.createRelationshipTo( cypher, RelTypes.KNOWS );
            rel.setProperty( "disclosure", "public" );
            Node smith = graphDb.createNode();
            smith.setProperty( "name", "Agent Smith" );
            smith.setProperty( "version", "1.0b" );
            smith.setProperty( "language", "C++" );
            rel = cypher.createRelationshipTo( smith, RelTypes.KNOWS );
            rel.setProperty( "disclosure", "secret" );
            rel.setProperty( "age", "6 months" );
            Node architect = graphDb.createNode();
            architect.setProperty( "name", "The Architect" );
            smith.createRelationshipTo( architect, RelTypes.CODED_BY );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    /**
     * Get the Neo node. (a.k.a. Thomas Anderson node)
     * 
     * @return the Neo node
     */
    private Node getNeoNode()
    {
        return graphDb.getReferenceNode()
        .getSingleRelationship( RelTypes.NEO_NODE, Direction.OUTGOING )
        .getEndNode();
    }

    public String printNeoFriends()
    {
        Node neoNode = getNeoNode();
        // START SNIPPET: friends-usage
        int numberOfFriends = 0;
        String output = neoNode.getProperty( "name" ) + "'s friends:\n";
        Traverser friendsTraverser = getFriends( neoNode );
        for ( Node friendNode : friendsTraverser )
        {
            output += "At depth " +
                        friendsTraverser.currentPosition().depth() + 
                        " => " + 
                        friendNode.getProperty( "name" ) + "\n";
            numberOfFriends++;
        }
        output += "Number of friends found: " + numberOfFriends + "\n";
        // END SNIPPET: friends-usage
        return output;
    }

    // START SNIPPET: get-friends
    private static Traverser getFriends( final Node person )
    {
        return person.traverse( Order.BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH,
                ReturnableEvaluator.ALL_BUT_START_NODE, RelTypes.KNOWS,
                Direction.OUTGOING );
    }
    // END SNIPPET: get-friends

    public String printMatrixHackers()
    {
        // START SNIPPET: find--hackers-usage
        String output = "Hackers:\n";
        int numberOfHackers = 0;
        Traverser traverser = findHackers( getNeoNode() );
        for ( Node hackerNode : traverser )
        {
            output += "At depth " +
                        traverser.currentPosition().depth() +
                        " => " + 
                        hackerNode.getProperty( "name" ) + "\n";
            numberOfHackers++;
        }
        output += "Number of hackers found: " + numberOfHackers + "\n";
        // END SNIPPET: find--hackers-usage
        return output;
        // assertEquals( 1, numberOfHackers );
    }

    // START SNIPPET: find-hackers
    private static Traverser findHackers( final Node startNode )
    {
        return startNode.traverse( Order.BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH, new ReturnableEvaluator()
        {
            @Override
            public boolean isReturnableNode(
                    final TraversalPosition currentPos )
            {
                return !currentPos.isStartNode()
                && currentPos.lastRelationshipTraversed()
                .isType( RelTypes.CODED_BY );
            }
        }, RelTypes.CODED_BY, Direction.OUTGOING, RelTypes.KNOWS,
        Direction.OUTGOING );
    }
    // END SNIPPET: find-hackers

    private void registerShutdownHook()
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime()
        .addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }

    private static void deleteFileOrDirectory( final File file )
    {
        if ( !file.exists() )
        {
            return;
        }

        if ( file.isDirectory() )
        {
            for ( File child : file.listFiles() )
            {
                deleteFileOrDirectory( child );
            }
        }
        else
        {
            file.delete();
        }
    }
}
