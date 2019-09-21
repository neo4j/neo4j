/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.function.Function;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.RelationshipType.withName;

class TestMultiRelTypesAndDirections extends TraversalTestBase
{
    private static final RelationshipType ONE = withName( "ONE" );

    @BeforeEach
    void setupGraph()
    {
        createGraph( "A ONE B", "B ONE C", "A TWO C" );
    }

    @Test
    void testCIsReturnedOnDepthTwoDepthFirst()
    {
        testCIsReturnedOnDepthTwo( transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void testCIsReturnedOnDepthTwoBreadthFirst()
    {
        testCIsReturnedOnDepthTwo( transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void testCIsReturnedOnDepthTwo( Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = beginTx() )
        {
            final TraversalDescription description = traversalFactory.apply( transaction )
                    .expand( PathExpanders.forTypeAndDirection( ONE, OUTGOING ) );
            int i = 0;
            for ( Path position : description.traverse( transaction.getNodeById( node( "A" ).getId() ) ) )
            {
                assertEquals( i++, position.length() );
            }
        }
    }
}
