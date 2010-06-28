package org.neo4j.kernel.impl.traversal;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.TraversalFactory;

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
        description = description.filter( TraversalFactory.returnAllButStartNode() ).prune(
                TraversalFactory.pruneAfterDepth( 1 ) );
        expectNodes( description.traverse( getNodeWithName( "3" ) ), "1", "2" );
    }
    
    @Test
    public void shouldGetBothNodesOnDepthOneForDepthFirst()
    {
        shouldGetBothNodesOnDepthOne( new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void shouldGetBothNodesOnDepthOneForBreadthFirst()
    {
        shouldGetBothNodesOnDepthOne( new TraversalDescriptionImpl().breadthFirst() );
    }
}
