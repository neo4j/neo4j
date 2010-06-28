package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.commons.Predicate;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.kernel.TraversalFactory;

public class DepthPitfallGraphTest extends AbstractTestBase
{
    /* Layout:
     *    _(2)--__
     *   /        \
     * (1)-(3)-_   (6)
     *  |\_     \  /
     *  |  (4)__ \/
     *  \_______(5)
     */
    private static final String[] THE_WORLD_AS_WE_KNOW_IT = new String[] {
            "1 TO 2", "1 TO 3", "1 TO 4", "5 TO 3", "1 TO 5", "4 TO 5",
            "2 TO 6", "5 TO 6" };
    private static final String[] NODE_UNIQUE_PATHS = new String[] { "1",
            "1,2", "1,2,6", "1,2,6,5", "1,2,6,5,3", "1,2,6,5,4", "1,3",
            "1,3,5", "1,3,5,4", "1,3,5,6", "1,3,5,6,2", "1,4", "1,4,5",
            "1,4,5,3", "1,4,5,6", "1,4,5,6,2", "1,5", "1,5,3", "1,5,4",
            "1,5,6", "1,5,6,2" };
    private static final String[] RELATIONSHIP_UNIQUE_EXTRA_PATHS = new String[] {
            "1,2,6,5,1", "1,2,6,5,1,3", "1,2,6,5,1,3,5", "1,2,6,5,1,3,5,4",
            "1,2,6,5,1,3,5,4,1", "1,2,6,5,1,4", "1,2,6,5,1,4,5",
            "1,2,6,5,1,4,5,3", "1,2,6,5,1,4,5,3,1", "1,2,6,5,3,1",
            "1,2,6,5,3,1,4", "1,2,6,5,3,1,4,5", "1,2,6,5,3,1,4,5,1",
            "1,2,6,5,3,1,5", "1,2,6,5,3,1,5,4", "1,2,6,5,3,1,5,4,1",
            "1,2,6,5,4,1", "1,2,6,5,4,1,3", "1,2,6,5,4,1,3,5",
            "1,2,6,5,4,1,3,5,1", "1,2,6,5,4,1,5", "1,2,6,5,4,1,5,3",
            "1,2,6,5,4,1,5,3,1", "1,3,5,1", "1,3,5,1,2", "1,3,5,1,2,6",
            "1,3,5,1,2,6,5", "1,3,5,1,2,6,5,4", "1,3,5,1,2,6,5,4,1",
            "1,3,5,1,4", "1,3,5,1,4,5", "1,3,5,1,4,5,6", "1,3,5,1,4,5,6,2",
            "1,3,5,1,4,5,6,2,1", "1,3,5,4,1", "1,3,5,4,1,2", "1,3,5,4,1,2,6",
            "1,3,5,4,1,2,6,5", "1,3,5,4,1,2,6,5,1", "1,3,5,4,1,5",
            "1,3,5,4,1,5,6", "1,3,5,4,1,5,6,2", "1,3,5,4,1,5,6,2,1",
            "1,3,5,6,2,1", "1,3,5,6,2,1,4", "1,3,5,6,2,1,4,5",
            "1,3,5,6,2,1,4,5,1", "1,3,5,6,2,1,5", "1,3,5,6,2,1,5,4",
            "1,3,5,6,2,1,5,4,1", "1,4,5,1", "1,4,5,1,2", "1,4,5,1,2,6",
            "1,4,5,1,2,6,5", "1,4,5,1,2,6,5,3", "1,4,5,1,2,6,5,3,1",
            "1,4,5,1,3", "1,4,5,1,3,5", "1,4,5,1,3,5,6", "1,4,5,1,3,5,6,2",
            "1,4,5,1,3,5,6,2,1", "1,4,5,3,1", "1,4,5,3,1,2", "1,4,5,3,1,2,6",
            "1,4,5,3,1,2,6,5", "1,4,5,3,1,2,6,5,1", "1,4,5,3,1,5",
            "1,4,5,3,1,5,6", "1,4,5,3,1,5,6,2", "1,4,5,3,1,5,6,2,1",
            "1,4,5,6,2,1", "1,4,5,6,2,1,3", "1,4,5,6,2,1,3,5",
            "1,4,5,6,2,1,3,5,1", "1,4,5,6,2,1,5", "1,4,5,6,2,1,5,3",
            "1,4,5,6,2,1,5,3,1", "1,5,3,1", "1,5,3,1,2", "1,5,3,1,2,6",
            "1,5,3,1,2,6,5", "1,5,3,1,2,6,5,4", "1,5,3,1,2,6,5,4,1",
            "1,5,3,1,4", "1,5,3,1,4,5", "1,5,3,1,4,5,6", "1,5,3,1,4,5,6,2",
            "1,5,3,1,4,5,6,2,1", "1,5,4,1", "1,5,4,1,2", "1,5,4,1,2,6",
            "1,5,4,1,2,6,5", "1,5,4,1,2,6,5,3", "1,5,4,1,2,6,5,3,1",
            "1,5,4,1,3", "1,5,4,1,3,5", "1,5,4,1,3,5,6", "1,5,4,1,3,5,6,2",
            "1,5,4,1,3,5,6,2,1", "1,5,6,2,1", "1,5,6,2,1,3", "1,5,6,2,1,3,5",
            "1,5,6,2,1,3,5,4", "1,5,6,2,1,3,5,4,1", "1,5,6,2,1,4",
            "1,5,6,2,1,4,5", "1,5,6,2,1,4,5,3", "1,5,6,2,1,4,5,3,1" };

    @BeforeClass
    public static void setup()
    {
        createGraph( THE_WORLD_AS_WE_KNOW_IT );
    }

    @Test
    public void testSmallestPossibleInit() throws Exception
    {
        Traverser traversal = new TraversalDescriptionImpl().traverse( referenceNode() );
        int count = 0;
        for ( Position position : traversal )
        {
            count++;
            assertNotNull( position );
            assertNotNull( position.node() );
            if ( !position.atStartNode() )
            {
                assertNotNull( position.lastRelationship() );
                assertTrue( position.depth() > 0 );
            }
            else
            {
                assertEquals( 0, position.depth() );
            }
            assertNotNull( position.path() );
            assertEquals( position.depth(), position.path().length() );
        }
        assertFalse( "empty traversal", count == 0 );
    }

    @Test
    public void testAllNodesAreReturnedOnceDepthFirst() throws Exception
    {
        testAllNodesAreReturnedOnce( new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void testAllNodesAreReturnedOnceBreadthFirst() throws Exception
    {
        testAllNodesAreReturnedOnce( new TraversalDescriptionImpl().breadthFirst() );
    }

    private void testAllNodesAreReturnedOnce( TraversalDescription traversal )
    {
        Traverser traverser = traversal.uniqueness( Uniqueness.NODE_GLOBAL ).traverse(
                referenceNode() );

        expectNodes( traverser, "1", "2", "3", "4", "5", "6" );
    }

    @Test
    public void testNodesAreReturnedOnceWhenSufficientRecentlyUniqueDepthFirst()
            throws Exception
    {
        testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
                new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void testNodesAreReturnedOnceWhenSufficientRecentlyUniqueBreadthFirst()
            throws Exception
    {
        testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
                new TraversalDescriptionImpl().breadthFirst() );
    }

    private void testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
            TraversalDescription description )
    {
        Traverser traverser = description.uniqueness( Uniqueness.NODE_RECENT, 6 ).traverse(
                referenceNode() );

        expectNodes( traverser, "1", "2", "3", "4", "5", "6" );
    }

    @Test
    public void testAllRelationshipsAreReturnedOnceDepthFirst()
            throws Exception
    {
        testAllRelationshipsAreReturnedOnce( new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void testAllRelationshipsAreReturnedOnceBreadthFirst()
            throws Exception
    {
        testAllRelationshipsAreReturnedOnce( new TraversalDescriptionImpl().breadthFirst() );
    }

    private void testAllRelationshipsAreReturnedOnce(
            TraversalDescription description ) throws Exception
    {
        Traverser traverser = new TraversalDescriptionImpl().uniqueness(
                Uniqueness.RELATIONSHIP_GLOBAL ).traverse( referenceNode() );

        expectRelationships( traverser, THE_WORLD_AS_WE_KNOW_IT );
    }

    @Test
    public void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUniqueDepthFirst()
            throws Exception
    {
        testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
                new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUniqueBreadthFirst()
            throws Exception
    {
        testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
                new TraversalDescriptionImpl().breadthFirst() );
    }
    
    private void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
            TraversalDescription description ) throws Exception
    {
        Traverser traverser = description.uniqueness(
                Uniqueness.RELATIONSHIP_RECENT, THE_WORLD_AS_WE_KNOW_IT.length ).traverse(
                referenceNode() );

        expectRelationships( traverser, THE_WORLD_AS_WE_KNOW_IT );
    }

    @Test
    public void testAllUniqueNodePathsAreReturnedDepthFirst() throws Exception
    {
        testAllUniqueNodePathsAreReturned( new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void testAllUniqueNodePathsAreReturnedBreadthFirst() throws Exception
    {
        testAllUniqueNodePathsAreReturned( new TraversalDescriptionImpl().breadthFirst() );
    }
    
    private void testAllUniqueNodePathsAreReturned( TraversalDescription description )
            throws Exception
    {
        Traverser traverser = description.uniqueness(
                Uniqueness.NODE_PATH ).traverse( referenceNode() );

        expectPaths( traverser, NODE_UNIQUE_PATHS );
    }
    
    @Test
    public void testAllUniqueRelationshipPathsAreReturnedDepthFirst() throws Exception
    {
        testAllUniqueRelationshipPathsAreReturned( new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void testAllUniqueRelationshipPathsAreReturnedBreadthFirst() throws Exception
    {
        testAllUniqueRelationshipPathsAreReturned( new TraversalDescriptionImpl().breadthFirst() );
    }
    
    private void testAllUniqueRelationshipPathsAreReturned( TraversalDescription description )
            throws Exception
    {
        Set<String> expected = new HashSet<String>(
                Arrays.asList( NODE_UNIQUE_PATHS ) );
        expected.addAll( Arrays.asList( RELATIONSHIP_UNIQUE_EXTRA_PATHS ) );

        Traverser traverser = description.uniqueness(
                Uniqueness.RELATIONSHIP_PATH ).traverse( referenceNode() );

        expectPaths( traverser, expected );
    }
    
    @Test
    public void canPruneTraversalAtSpecificDepthDepthFirst()
    {
        canPruneTraversalAtSpecificDepth( new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void canPruneTraversalAtSpecificDepthBreadthFirst()
    {
        canPruneTraversalAtSpecificDepth( new TraversalDescriptionImpl().breadthFirst() );
    }
    
    private void canPruneTraversalAtSpecificDepth( TraversalDescription description )
    {
        Traverser traverser = description.uniqueness(
                Uniqueness.NONE ).prune( TraversalFactory.pruneAfterDepth( 1 ) ).traverse(
                referenceNode() );

        expectNodes( traverser, "1", "2", "3", "4", "5" );
    }
    
    @Test
    public void canPreFilterNodesDepthFirst()
    {
        canPreFilterNodes( new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void canPreFilterNodesBreadthFirst()
    {
        canPreFilterNodes( new TraversalDescriptionImpl().breadthFirst() );
    }
    
    private void canPreFilterNodes( TraversalDescription description )
    {
        Traverser traverser = description.uniqueness(
                Uniqueness.NONE ).prune( TraversalFactory.pruneAfterDepth( 2 ) ).filter(
                new Predicate<Position>()
                {
                    public boolean accept( Position position )
                    {
                        return position.depth() == 2;
                    }
                } ).traverse( referenceNode() );

        expectPaths( traverser, "1,2,6", "1,3,5", "1,4,5", "1,5,3", "1,5,4",
                "1,5,6" );
    }
}
