package org.neo4j.examples.apoc;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.util.shell.LocalNeoShellServer;
import org.neo4j.util.shell.SameJvmClient;
import org.neo4j.util.shell.ShellServer;

public class ShellExample
{
    private static final String NEO_DB_PATH = "neo-store";
    private static final String USERNAME_KEY = "username";
    
    private static NeoService neo;
    
    private static enum RelTypes implements RelationshipType
    {
        USERS_REFERENCE,
        USER,
    }
    
    public static void main( String[] args ) throws Exception
    {
        neo = new EmbeddedNeo( NEO_DB_PATH );
        registerShutdownHookForNeo();
        
        createExampleNodeSpace();
        boolean trueForLocal = waitForUserInput( "Would you like to start a " +
            "local shell instance or enable neo to accept remote " +
            "connections [l/r]? " ).equalsIgnoreCase( "l" );
        if ( trueForLocal )
        {
            startLocalShell();
        }
        else
        {
            startRemoteShellAndWait();
        }
        deleteExampleNodeSpace();
        
        System.out.println( "Shutting down..." );
        shutdown();
    }
    
    private static void startLocalShell() throws Exception
    {
        ShellServer shellServer = new LocalNeoShellServer( neo );
        new SameJvmClient( shellServer ).grabPrompt();
        shellServer.shutdown();
    }

    private static void startRemoteShellAndWait() throws Exception
    {
        neo.enableRemoteShell();
        waitForUserInput( "Remote shell enabled, connect to it by running:\n" +
            "java -jar shell.jar\n" +
            "\nWhen you're done playing around, just press any key " +
            "in this terminal" );
    }
    
    private static String waitForUserInput( String textToSystemOut )
        throws Exception
    {
        System.out.print( textToSystemOut );
        return new BufferedReader(
            new InputStreamReader( System.in ) ).readLine();
    }

    private static void createExampleNodeSpace()
    {
        Transaction tx = neo.beginTx();
        try
        {
            // Create users sub reference node (see design guide lines on
            // http://wiki.neo4j.org)
            System.out.println( "Creating example node space..." );
            Node usersReferenceNode = neo.createNode();
            neo.getReferenceNode().createRelationshipTo( usersReferenceNode,
                RelTypes.USERS_REFERENCE );
            
            // Create some users and index their names with the IndexService
            for ( int id = 0; id < 100; id++ )
            {
                Node userNode = createUser( formUserName( id ) ); 
                usersReferenceNode.createRelationshipTo( userNode,
                    RelTypes.USER );
            }
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
    
    private static void deleteExampleNodeSpace()
    {
        Transaction tx = neo.beginTx();
        try
        {
            // Delete the persons and remove them from the index
            System.out.println( "Deleting example node space..." );
            Node usersReferenceNode =
                neo.getReferenceNode().getSingleRelationship(
                    RelTypes.USERS_REFERENCE, Direction.OUTGOING ).getEndNode();
            for ( Relationship relationship :
                usersReferenceNode.getRelationships( RelTypes.USER,
                    Direction.OUTGOING ) )
            {
                Node user = relationship.getEndNode();
                user.delete();
                relationship.delete();
            }
            usersReferenceNode.getSingleRelationship( RelTypes.USERS_REFERENCE,
                Direction.INCOMING ).delete();
            usersReferenceNode.delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
    
    private static void shutdown()
    {
        neo.shutdown();
    }
    
    private static void registerShutdownHookForNeo()
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdown();
            }
        } );
    }
    
    private static String formUserName( int id )
    {
        return "user" + id + "@neo4j.org";
    }

    private static Node createUser( String username )
    {
        Node node = neo.createNode();
        node.setProperty( USERNAME_KEY, username );
        return node;
    }
}
