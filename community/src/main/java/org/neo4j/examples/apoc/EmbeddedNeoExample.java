package org.neo4j.examples.apoc;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;

public class EmbeddedNeoExample
{
    private static final String NEO_DB_PATH = "neo-store";
    private static final String NAME_KEY = "name";
    
    private static enum ExampleRelationshipTypes implements RelationshipType
    {
        EXAMPLE
    }

    public static void main( String[] args )
    {
        final NeoService neo = new EmbeddedNeo( NEO_DB_PATH );
        registerShutdownHookForNeo( neo );
        
        // Encapsulate some operations in a transaction
        Transaction tx = neo.beginTx();
        try
        {
            Node firstNode = neo.createNode();
            firstNode.setProperty( NAME_KEY, "Hello" );
            
            Node secondNode = neo.createNode();
            secondNode.setProperty( NAME_KEY, "World" );
            
            firstNode.createRelationshipTo( secondNode,
                ExampleRelationshipTypes.EXAMPLE );
            
            String greeting = firstNode.getProperty( NAME_KEY ) + " " +
                secondNode.getProperty( NAME_KEY );
            System.out.println( greeting );
            
            firstNode.getSingleRelationship(
                ExampleRelationshipTypes.EXAMPLE, Direction.OUTGOING ).delete();
            firstNode.delete();
            secondNode.delete();
            
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        System.out.println( "Shutting down..." );
        neo.shutdown();
    }
    
    private static void registerShutdownHookForNeo( final NeoService neo )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                neo.shutdown();
            }
        } );
    }
}
