package org.neo4j.kernel.impl.traversal;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ReturnableEvaluator;
import org.neo4j.graphdb.StopEvaluator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TraversalPosition;
import org.neo4j.graphdb.Traverser;
import org.neo4j.graphdb.Traverser.Order;

public class CircularGraphTest extends AbstractTestBase
{
    @BeforeClass
    public static void createTheGraph()
    {
        createGraph( "1 TO 2", "2 TO 3", "3 TO 1" );
    }
    
    @Test
    public void testCircularBug()
    {
        final long timestamp = 3;
        Transaction tx = beginTx();
        getNodeWithName( "2" ).setProperty( "timestamp", 1L );
        getNodeWithName( "3" ).setProperty( "timestamp", 2L );
        tx.success();
        tx.finish();
        
        final RelationshipType type = DynamicRelationshipType.withName( "TO" );
        Traverser t = referenceNode().traverse( Order.DEPTH_FIRST, new StopEvaluator()
        {
            public boolean isStopNode( TraversalPosition position )
            {
                Relationship last = position.lastRelationshipTraversed();
                if ( last != null && last.isType( type ) )
                {
                    Node node = position.currentNode();
                    if ( !node.hasProperty( "timestamp" ) ) new Exception().printStackTrace();
                    long currentTime = (Long) node.getProperty( "timestamp" );
                    return currentTime >= timestamp;
                }
                return false;
            }
        }, new ReturnableEvaluator()
        {
            public boolean isReturnableNode( TraversalPosition position )
            {
                Relationship last = position.lastRelationshipTraversed();
                if ( last != null && last.isType( type ) )
                {
                    return true;
                }
                return false;
            }
        }, type, Direction.OUTGOING );
        for ( Node node : t )
        {
            System.out.println( node );
        }
    }
}
