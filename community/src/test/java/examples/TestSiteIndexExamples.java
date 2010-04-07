package examples;

import static org.junit.Assert.assertEquals;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.commons.iterator.IterableWrapper;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphmatching.PatternMatch;
import org.neo4j.graphmatching.PatternMatcher;
import org.neo4j.graphmatching.PatternNode;
import org.neo4j.kernel.EmbeddedGraphDatabase;

/**
 * Example code for the index page of the component site.
 * 
 * @author Tobias Ivarsson
 */
public class TestSiteIndexExamples
{
    // START SNIPPET: findNodesWithRelationshipsTo
    public static Iterable<Node> findNodesWithRelationshipsTo(
            RelationshipType type, Node... nodes )
    {
        if ( nodes == null || nodes.length == 0 )
        {
            throw new IllegalArgumentException( "No nodes supplied" );
        }
        final PatternNode requested = new PatternNode();
        PatternNode anchor = null;
        for ( Node node : nodes )
        {
            PatternNode pattern = new PatternNode();
            pattern.setAssociation( node );
            pattern.createRelationshipTo( requested, type );
            if ( anchor == null )
            {
                anchor = pattern;
            }
        }
        PatternMatcher matcher = PatternMatcher.getMatcher();
        Iterable<PatternMatch> matches = matcher.match( anchor, nodes[0] );
        return new IterableWrapper<Node, PatternMatch>( matches )
        {
            @Override
            protected Node underlyingObjectToObject( PatternMatch match )
            {
                return match.getNodeFor( requested );
            }
        };
    }
    // END SNIPPET: findNodesWithRelationshipsTo

    @Test
    public void verifyFunctionalityOfFindNodesWithRelationshipsTo()
            throws Exception
    {
        final RelationshipType type = DynamicRelationshipType.withName( "RELATED" );
        Node[] nodes = createGraph( new GraphDefinition<Node[]>()
        {
            public Node[] create( GraphDatabaseService graphdb )
            {
                Node[] nodes = new Node[5];
                for ( int i = 0; i < nodes.length; i++ )
                {
                    nodes[i] = graphdb.createNode();
                }
                for ( int i = 0; i < 3; i++ )
                {
                    Node node = graphdb.createNode();
                    for ( int j = 0; j < nodes.length; j++ )
                    {
                        nodes[j].createRelationshipTo( node, type );
                    }
                }
                return nodes;
            }
        } );
        Transaction tx = graphDb.beginTx();
        try
        {
            assertEquals( 3,
                    count( findNodesWithRelationshipsTo( type, nodes ) ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private int count( Iterable<?> objects )
    {
        int count = 0;
        for ( @SuppressWarnings( "unused" ) Object object : objects )
        {
            count++;
        }
        return count;
    }

    private interface GraphDefinition<RESULT>
    {
        RESULT create( GraphDatabaseService graphdb );
    }

    private <T> T createGraph( GraphDefinition<T> definition )
    {
        final T result;
        Transaction tx = graphDb.beginTx();
        try
        {
            result = definition.create( graphDb );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        return result;
    }

    private static GraphDatabaseService graphDb;

    @BeforeClass
    public static void startGraphDatabase()
    {
        graphDb = new EmbeddedGraphDatabase( "target/var/db" );
    }

    @AfterClass
    public static void shutdownGraphDatabase()
    {
        graphDb.shutdown();
        graphDb = null;
    }
}
