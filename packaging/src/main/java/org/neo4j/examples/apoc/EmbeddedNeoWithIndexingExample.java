package org.neo4j.examples.apoc;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.index.IndexService;
import org.neo4j.index.lucene.LuceneIndexService;
import org.neo4j.kernel.EmbeddedGraphDatabase;

public class EmbeddedNeoWithIndexingExample
{
    private static final String DB_PATH = "neo4j-store";
    private static final String USERNAME_KEY = "username";
    
    private static GraphDatabaseService graphDb;
    private static IndexService indexService;
    
    private static enum RelTypes implements RelationshipType
    {
        USERS_REFERENCE,
        USER,
    }

    public static void main( String[] args )
    {
        graphDb = new EmbeddedGraphDatabase( DB_PATH );
        indexService = new LuceneIndexService( graphDb );
        registerShutdownHook();
        
        Transaction tx = graphDb.beginTx();
        try
        {
            // Create users sub reference node (see design guidelines on
            // http://wiki.neo4j.org)
            Node usersReferenceNode = graphDb.createNode();
            graphDb.getReferenceNode().createRelationshipTo( usersReferenceNode,
                RelTypes.USERS_REFERENCE );
            
            // Create some users and index their names with the IndexService
            for ( int id = 0; id < 100; id++ )
            {
                Node userNode = createAndIndexUser( idToUserName( id ) ); 
                usersReferenceNode.createRelationshipTo( userNode,
                    RelTypes.USER );
            }
            System.out.println( "Users created" );
            
            // Find a user through the search index
            int idToFind = 45;
            Node foundUser = indexService.getSingleNode( USERNAME_KEY,
                idToUserName( idToFind ) );
            System.out.println( "The username of user " + idToFind + " is " +
                foundUser.getProperty( USERNAME_KEY ) );
            
            // Delete the persons and remove them from the index
            for ( Relationship relationship :
                usersReferenceNode.getRelationships( RelTypes.USER,
                    Direction.OUTGOING ) )
            {
                Node user = relationship.getEndNode();
                indexService.removeIndex( user, USERNAME_KEY,
                    user.getProperty( USERNAME_KEY ) );
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
        System.out.println( "Shutting down..." );
        shutdown();
    }
    
    private static void shutdown()
    {
        indexService.shutdown();
        graphDb.shutdown();
    }
    
    private static String idToUserName( int id )
    {
        return "user" + id + "@neo4j.org";
    }

    private static Node createAndIndexUser( String username )
    {
        Node node = graphDb.createNode();
        node.setProperty( USERNAME_KEY, username );
        indexService.index( node, USERNAME_KEY, username );
        return node;
    }
    
    private static void registerShutdownHook()
    {
        // Registers a shutdown hook for the Neo4j and index service instances
        // so that it shuts down nicely when the VM exits (even if you
        // "Ctrl-C" the running example before it's completed)
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                shutdown();
            }
        } );
    }    
}
