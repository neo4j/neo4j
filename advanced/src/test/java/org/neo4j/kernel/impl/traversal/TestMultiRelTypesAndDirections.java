package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.TraversalDescription;

public class TestMultiRelTypesAndDirections extends AbstractTestBase
{
    private static final RelationshipType ONE = DynamicRelationshipType.withName( "ONE" );

    @BeforeClass
    public static void setupGraph()
    {
        createGraph( "A ONE B", "B ONE C", "A TWO C" );
    }

    @Test
    public void testCIsReturnedOnDepthTwoDepthFirst()
    {
        testCIsReturnedOnDepthTwo( new TraversalDescriptionImpl().depthFirst() );
    }

    @Test
    public void testCIsReturnedOnDepthTwoBreadthFirst()
    {
        testCIsReturnedOnDepthTwo( new TraversalDescriptionImpl().breadthFirst() );
    }

    private void testCIsReturnedOnDepthTwo( TraversalDescription description )
    {
        description = description.relationships(ONE, Direction.OUTGOING);
        int i = 0;
        for ( Path position : description.traverse( referenceNode() ) )
        {
            assertEquals( i++, position.length() );
        }
    }
}
