/**
 * Copyright (c) 2002-2012 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertEquals;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Traversal;

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
        testCIsReturnedOnDepthTwo( Traversal.description().depthFirst() );
    }

    @Test
    public void testCIsReturnedOnDepthTwoBreadthFirst()
    {
        testCIsReturnedOnDepthTwo( Traversal.description().breadthFirst() );
    }

    private void testCIsReturnedOnDepthTwo( TraversalDescription description )
    {
        description = description.relationships(ONE, Direction.OUTGOING);
        int i = 0;
        for ( Path position : description.traverse( node( "A" ) ) )
        {
            assertEquals( i++, position.length() );
        }
    }
}
