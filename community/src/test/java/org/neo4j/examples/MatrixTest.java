package org.neo4j.examples;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class MatrixTest
{
    public enum RelTypes implements RelationshipType
    {
        NEO_NODE,
        KNOWS,
        CODED_BY
    }

    private static final String MATRIX_DB = "target/matrix-db";
    private static GraphDatabaseService graphDb;

    @BeforeClass
    public static void setUp()
    {
        deleteFileOrDirectory( new File( MATRIX_DB ) );
        graphDb = new EmbeddedGraphDatabase( MATRIX_DB );
        registerShutdownHook();
        createNodespace();
    }

    @AfterClass
    public static void tearDown()
    {
        graphDb.shutdown();
    }

    private static void createNodespace()
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
    private static Node getNeoNode()
    {
        return graphDb.getReferenceNode().getSingleRelationship(
                RelTypes.NEO_NODE, Direction.OUTGOING ).getEndNode();
    }

    @Test
    public void printNeoFriends() throws Exception
    {
        Node neoNode = getNeoNode();
        System.out.println( neoNode.getProperty( "name" ) + "'s friends:" );
        // START SNIPPET: get-friends-usage
        Traverser friendsTraverser = getFriends( neoNode );
        int numberOfFriends = 0;
        for ( Path friendPath : friendsTraverser )
        {
            System.out.println( "At depth " + friendPath.length() + " => "
                                + friendPath.endNode().getProperty( "name" ) );
            numberOfFriends++;
        }
        // END SNIPPET: get-friends-usage
        assertEquals( 4, numberOfFriends );
    }

    // START SNIPPET: get-friends
    private static Traverser getFriends( final Node person )
    {
        TraversalDescription td = Traversal.description().breadthFirst().relationships(
                RelTypes.KNOWS, Direction.OUTGOING ).filter(
                Traversal.returnAllButStartNode() );
        return td.traverse( person );
    }
    // END SNIPPET: get-friends

    @Test
    public void printMatrixHackers() throws Exception
    {
        System.out.println( "Hackers:" );
        // START SNIPPET: find-hackers-usage
        Traverser traverser = findHackers( getNeoNode() );
        int numberOfHackers = 0;
        for ( Path hackerPath : traverser )
        {
            System.out.println( "At depth " + hackerPath.length() + " => "
                                + hackerPath.endNode().getProperty( "name" ) );
            numberOfHackers++;
        }
        // END SNIPPET: find-hackers-usage
        assertEquals( 1, numberOfHackers );
    }

    // START SNIPPET: find-hackers
    private static Traverser findHackers( final Node startNode )
    {
        TraversalDescription td = Traversal.description().breadthFirst().relationships(
                RelTypes.CODED_BY, Direction.OUTGOING ).relationships(
                RelTypes.KNOWS, Direction.OUTGOING ).filter(
                new Predicate<Path>()
                {
                    public boolean accept( final Path path )
                    {
                        Relationship rel = path.lastRelationship();
                        return rel != null && rel.isType( RelTypes.CODED_BY );
                    }
                } );
        return td.traverse( startNode );
    }
    // END SNIPPET: find-hackers

    @Test
    public void depthTwoTraversal()
    {
        // work in progress
        TraversalDescription td = Traversal.description().depthFirst().relationships(
                RelTypes.KNOWS ).uniqueness( Uniqueness.NONE ).prune(
                Traversal.pruneAfterDepth( 2 ) ).filter( new Predicate<Path>()
        {
            public boolean accept( Path item )
            {
                return item.length() > 1;
            }
        } );
        for ( Path path : td.traverse( getNeoNode() ) )
        {
            System.out.println( path );
        }
    }

    private static void registerShutdownHook()
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
