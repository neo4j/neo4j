package org.neo4j.examples.apoc;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.neo4j.api.core.Direction;
import org.neo4j.api.core.EmbeddedNeo;
import org.neo4j.api.core.NeoService;
import org.neo4j.api.core.Node;
import org.neo4j.api.core.Relationship;
import org.neo4j.api.core.RelationshipType;
import org.neo4j.api.core.Transaction;
import org.neo4j.shell.neo.LocalNeoShellServer;
import org.neo4j.shell.SameJvmClient;
import org.neo4j.shell.ShellServer;

public class ShellExample
{
    private static final String NEO_DB_PATH = "neo-store";
    private static final String USERNAME_KEY = "username";
    
    private static NeoService neo;
    
    private static enum RelTypes implements RelationshipType
    {
        USERS_REFERENCE,
        USER,
        KNOWS,
    }
    
    public static void main( String[] args ) throws Exception
    {
        registerShutdownHookForNeo();
        
        startNeo();
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
        
        System.out.println( "Shutting down..." );
        shutdown();
    }
    
    private static void startNeo()
    {
        neo = new EmbeddedNeo( NEO_DB_PATH );
    }
    
    private static void startLocalShell() throws Exception
    {
        shutdownNeo();
        
        ShellServer shellServer = new LocalNeoShellServer( NEO_DB_PATH, false );
        new SameJvmClient( shellServer ).grabPrompt();
        shellServer.shutdown();
        
        startNeo();
    }

    private static void startRemoteShellAndWait() throws Exception
    {
        neo.enableRemoteShell();
        waitForUserInput( "Remote shell enabled, connect to it by running:\n" +
            "java -jar lib/shell-<version>.jar\n" +
            "\nWhen you're done playing around, just press any key " +
            "in this terminal " );
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
            Random random = new Random();
            Node usersReferenceNode = neo.createNode();
            neo.getReferenceNode().createRelationshipTo( usersReferenceNode,
                RelTypes.USERS_REFERENCE );
            
            // Create some users and index their names with the IndexService
            List<Node> users = new ArrayList<Node>();
            for ( int id = 0; id < 100; id++ )
            {
                Node userNode = createUser( formUserName( id ) ); 
                usersReferenceNode.createRelationshipTo( userNode,
                    RelTypes.USER );
                if ( id > 10 )
                {
                    int numberOfFriends = random.nextInt( 5 );
                    Set<Node> knows = new HashSet<Node>();
                    for ( int i = 0; i < numberOfFriends; i++ )
                    {
                        Node friend = users.get( random.nextInt(
                            users.size() ) );
                        if ( knows.add( friend ) )
                        {
                            userNode.createRelationshipTo( friend,
                                RelTypes.KNOWS );
                        }
                    }
                }
                users.add( userNode );
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
                for ( Relationship knowsRelationship : user.getRelationships(
                    RelTypes.KNOWS ) )
                {
                    knowsRelationship.delete();
                }
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
    
    private static void shutdownNeo()
    {
        neo.shutdown();
        neo = null;
    }
    
    private static void shutdown()
    {
        if ( neo != null )
        {
            deleteExampleNodeSpace();
            shutdownNeo();
        }
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
