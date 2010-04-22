package org.neo4j.graphdb.traversal;

import org.junit.BeforeClass;
import org.junit.Test;

import common.AbstractTestBase;

public class DepthOneTraversalTest extends AbstractTestBase
{
    @BeforeClass
    public static void createTheGraph()
    {
        createGraph( "0 ROOT 1", "1 KNOWS 2", "2 KNOWS 3", "2 KNOWS 4",
                "4 KNOWS 5", "5 KNOWS 6", "3 KNOWS 1" );
    }
    
    private void shouldGetBothNodesOnDepthOne( TraversalDescription description )
    {
        description = description.filter( ReturnFilter.ALL_BUT_START_NODE ).prune(
                new PruneAfterDepth( 1 ) );
        expectNodes( description.breadthFirst().traverse( getNodeWithName( "3" ) ), "1", "2" );
    }
    
    @Test
    public void shouldGetBothNodesOnDepthOneForDepthFirst()
    {
        shouldGetBothNodesOnDepthOne( new TraversalDescription().depthFirst() );
    }

    @Test
    public void shouldGetBothNodesOnDepthOneForBreadthFirst()
    {
        shouldGetBothNodesOnDepthOne( new TraversalDescription().breadthFirst() );
    }
}
