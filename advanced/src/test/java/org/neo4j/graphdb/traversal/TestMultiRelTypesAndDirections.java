package org.neo4j.graphdb.traversal;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;

import common.AbstractTestBase;

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
        testCIsReturnedOnDepthTwo( new TraversalDescription().depthFirst() );
    }
    
    @Test
    public void testCIsReturnedOnDepthTwoBreadthFirst()
    {
        testCIsReturnedOnDepthTwo( new TraversalDescription().breadthFirst() );
    }

    private void testCIsReturnedOnDepthTwo( TraversalDescription description )
    {
        description = description.relationships(ONE, Direction.OUTGOING);
        int i = 0;
        for ( Position position : description.traverse( referenceNode() ) )
        {
            assertEquals( i++, position.depth() );
        }
    }
}
