package org.neo4j.remote.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.junit.Test;
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
import org.neo4j.index.IndexService;
import org.neo4j.remote.AbstractTestBase;
import org.neo4j.remote.RemoteGraphDatabase;

public class TheMatrixTest extends AbstractTestBase
{
    public TheMatrixTest( Callable<RemoteGraphDatabase> factory )
    {
        super( factory );
    }

    public @Test
    void testTheMatrix() throws Exception
    {
        Transaction tx = graphDb().beginTx();
        try
        {
            defineMatrix( graphDb(), indexService() );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
        tx = graphDb().beginTx();
        try
        {
            verifyFriendsOf( indexService().getSingleNode( "name",
                    "Thomas Andersson" ) );
            verifyHackersInNetworkOf( indexService().getSingleNode( "name",
                    "Thomas Andersson" ) );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private static enum MatrixRelation implements RelationshipType
    {
        KNOWS,
        CODED_BY,
        LOVES
    }

    private static void defineMatrix( GraphDatabaseService graphDb,
            IndexService index ) throws Exception
    {
        // Define nodes
        Node mrAndersson, morpheus, trinity, cypher, agentSmith, theArchitect;
        mrAndersson = graphDb.createNode();
        morpheus = graphDb.createNode();
        trinity = graphDb.createNode();
        cypher = graphDb.createNode();
        agentSmith = graphDb.createNode();
        theArchitect = graphDb.createNode();
        // Define relationships
        @SuppressWarnings( "unused" ) Relationship aKm, aKt, mKt, mKc, cKs, sCa, tLa;
        aKm = mrAndersson.createRelationshipTo( morpheus, MatrixRelation.KNOWS );
        aKt = mrAndersson.createRelationshipTo( trinity, MatrixRelation.KNOWS );
        mKt = morpheus.createRelationshipTo( trinity, MatrixRelation.KNOWS );
        mKc = morpheus.createRelationshipTo( cypher, MatrixRelation.KNOWS );
        cKs = cypher.createRelationshipTo( agentSmith, MatrixRelation.KNOWS );
        sCa = agentSmith.createRelationshipTo( theArchitect,
                MatrixRelation.CODED_BY );
        tLa = trinity.createRelationshipTo( mrAndersson, MatrixRelation.LOVES );
        // Define node properties
        mrAndersson.setProperty( "name", "Thomas Andersson" );
        morpheus.setProperty( "name", "Morpheus" );
        trinity.setProperty( "name", "Trinity" );
        cypher.setProperty( "name", "Cypher" );
        agentSmith.setProperty( "name", "Agent Smith" );
        theArchitect.setProperty( "name", "The Architect" );
        // Define relationship properties
        // Index nodes
        indexNodes( index, "name", mrAndersson, morpheus, trinity, cypher,
                agentSmith, theArchitect );
    }

    private static void indexNodes( IndexService index, String key,
            Node... nodes )
    {
        for ( Node node : nodes )
        {
            index.index( node, key, node.getProperty( key ) );
        }
    }

    @SuppressWarnings( "deprecation" )
    private static void verifyFriendsOf( Node thomas ) throws Exception
    {
        Traverser traverser = thomas.traverse( Order.BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH,
                ReturnableEvaluator.ALL_BUT_START_NODE, MatrixRelation.KNOWS,
                Direction.OUTGOING );
        Set<String> actual = new HashSet<String>();
        for ( Node friend : traverser )
        {
            assertTrue( "Same friend added twice.",
                    actual.add( (String) friend.getProperty( "name" ) ) );
        }
        assertEquals( "Thomas Anderssons friends are incorrect.",
                new HashSet<String>( Arrays.asList( "Trinity", "Morpheus",
                        "Cypher", "Agent Smith" ) ), actual );
    }

    @SuppressWarnings( { "serial", "deprecation" } )
    private static void verifyHackersInNetworkOf( Node thomas )
            throws Exception
    {
        Traverser traverser = thomas.traverse( Order.BREADTH_FIRST,
                StopEvaluator.END_OF_GRAPH, new ReturnableEvaluator()
                {
                    public boolean isReturnableNode( TraversalPosition pos )
                    {
                        return pos.notStartNode()
                               && pos.lastRelationshipTraversed().isType(
                                       MatrixRelation.CODED_BY );
                    }
                }, MatrixRelation.CODED_BY, Direction.OUTGOING,
                MatrixRelation.KNOWS, Direction.OUTGOING );
        Map<String, Integer> actual = new HashMap<String, Integer>();
        for ( Node hacker : traverser )
        {
            assertNull( "Same hacker found twice.", actual.put(
                    (String) hacker.getProperty( "name" ),
                    traverser.currentPosition().depth() ) );
        }
        assertEquals( "", new HashMap<String, Integer>()
        {
            {
                put( "The Architect", 4 );
            }
        }, actual );
    }
}
