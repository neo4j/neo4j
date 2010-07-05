package org.neo4j.kernel;

import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;

import org.junit.Test;
import org.neo4j.graphdb.RelationshipType;

public class TestTraversalFactory
{
    private static RelationshipType T1 = withName( "T1" ),
            T2 = withName( "T2" ), T3 = withName( "T3" );

    @Test
    public void canCreateExpanderWithMultipleTypesAndDirections()
    {
        assertNotNull( TraversalFactory.expanderForTypes( T1, INCOMING, T2,
                OUTGOING, T3, BOTH ) );
    }
}
