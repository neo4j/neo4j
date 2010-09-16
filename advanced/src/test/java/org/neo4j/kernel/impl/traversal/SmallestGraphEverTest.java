package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertFalse;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

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
        execute( Traversal.description().depthFirst(), Uniqueness.NONE );
    }

    @Test
    public void testUnrestrictedTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.NONE );
    }

    @Test
    public void testNodeGlobalTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    public void testNodeGlobalTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    public void testRelationshipGlobalTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    public void testRelationshipGlobalTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    public void testNodePathTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    public void testNodePathTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    public void testRelationshipPathTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    public void testRelationshipPathTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    public void testNodeRecentTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    public void testNodeRecentTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    public void testRelationshipRecentTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    @Test
    public void testRelationshipRecentTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    private void execute( TraversalDescription traversal, Uniqueness uniqueness )
    {
        Traverser traverser = traversal.uniqueness( uniqueness ).traverse(
                referenceNode() );
        assertFalse( "empty traversal", IteratorUtil.count( traverser ) == 0 );
    }
}
