package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertFalse;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

public class SmallestGraphEverTest extends AbstractTestBase
{
    @BeforeClass
    public static void setup()
    {
        createGraph( "1 TO 2" );
    }

    @Test
    public void testUnrestrictedTraversalCanFinishDepthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().depthFirst(), Uniqueness.NONE );
    }

    @Test
    public void testUnrestrictedTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().breadthFirst(), Uniqueness.NONE );
    }
    
    @Test
    public void testNodeGlobalTraversalCanFinishDepthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().depthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    public void testNodeGlobalTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().breadthFirst(), Uniqueness.NODE_GLOBAL );
    }
    
    @Test
    public void testRelationshipGlobalTraversalCanFinishDepthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().depthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    public void testRelationshipGlobalTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().breadthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }
    
    @Test
    public void testNodePathTraversalCanFinishDepthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().depthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    public void testNodePathTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().breadthFirst(), Uniqueness.NODE_PATH );
    }
    
    @Test
    public void testRelationshipPathTraversalCanFinishDepthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().depthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    public void testRelationshipPathTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().breadthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }
    
    @Test
    public void testNodeRecentTraversalCanFinishDepthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().depthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    public void testNodeRecentTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().breadthFirst(), Uniqueness.NODE_RECENT );
    }
    
    @Test
    public void testRelationshipRecentTraversalCanFinishDepthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().depthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    @Test
    public void testRelationshipRecentTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( new TraversalDescriptionImpl().breadthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }
    
    private void execute( TraversalDescription traversal, Uniqueness uniqueness )
    {
        Traverser traverser = traversal.uniqueness( uniqueness ).traverse(
                referenceNode() );
        int count = 0;
        for ( Position position : traverser )
        {
            count++;
        }
        assertFalse( "empty traversal", count == 0 );
    }
}
