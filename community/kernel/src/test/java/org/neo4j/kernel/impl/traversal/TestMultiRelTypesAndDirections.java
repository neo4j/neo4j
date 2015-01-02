/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.DynamicRelationshipType.withName;
import static org.neo4j.kernel.Traversal.expanderForTypes;
import static org.neo4j.kernel.Traversal.traversal;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;

public class TestMultiRelTypesAndDirections extends TraversalTestBase
{
    private static final RelationshipType ONE = withName( "ONE" );

    @Before
    public void setupGraph()
    {
        createGraph( "A ONE B", "B ONE C", "A TWO C" );
    }

    @Test
    public void testCIsReturnedOnDepthTwoDepthFirst()
    {
        testCIsReturnedOnDepthTwo( traversal().depthFirst() );
    }

    @Test
    public void testCIsReturnedOnDepthTwoBreadthFirst()
    {
        testCIsReturnedOnDepthTwo( traversal().breadthFirst() );
    }

    private void testCIsReturnedOnDepthTwo( TraversalDescription description )
    {
        Transaction transaction = beginTx();
        try
        {
            description = description.expand( expanderForTypes( ONE, OUTGOING ) );
            int i = 0;
            for ( Path position : description.traverse( node( "A" ) ) )
            {
                assertEquals( i++, position.length() );
            }
        }
        finally
        {
            transaction.finish();
        }
    }
}
