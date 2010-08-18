package org.neo4j.examples.apoc;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class EmbeddedNeo4j
{
    private static final String DB_PATH = "neo4j-store";
    private static final String NAME_KEY = "name";

    // START SNIPPET: createReltype
    private static enum ExampleRelationshipTypes implements RelationshipType
    {
        EXAMPLE
    }
    // END SNIPPET: createReltype

    public static void main( final String[] args )
    {
        // START SNIPPET: startDb
        GraphDatabaseService graphDb = new EmbeddedGraphDatabase( DB_PATH );
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb

        // START SNIPPET: operationsInATransaction
        // Encapsulate operations in a transaction
        Transaction tx = graphDb.beginTx();
        try
        {
            Node firstNode = graphDb.createNode();
            firstNode.setProperty( NAME_KEY, "Hello" );
            Node secondNode = graphDb.createNode();
            secondNode.setProperty( NAME_KEY, "World" );

            firstNode.createRelationshipTo( secondNode,
                ExampleRelationshipTypes.EXAMPLE );

            String greeting = firstNode.getProperty( NAME_KEY ) + " "
                + secondNode.getProperty( NAME_KEY );
            System.out.println( greeting );
            // END SNIPPET: operationsInATransaction

            // START SNIPPET: removingData
            // let's remove the data before committing
            firstNode.getSingleRelationship( ExampleRelationshipTypes.EXAMPLE,
                    Direction.OUTGOING ).delete();
            firstNode.delete();
            secondNode.delete();

            tx.success();
        }
        finally
        {
            tx.finish();
        }
        // END SNIPPET: removingData

        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }

    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
}
