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

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.internal.helpers.collection.Iterables;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.neo4j.graphdb.traversal.Evaluators.excludeStartPosition;

class SmallestGraphEverTest extends TraversalTestBase
{
    @BeforeEach
    void setup()
    {
        createGraph( "1 TO 2" );
    }

    @Test
    void testUnrestrictedTraversalCanFinishDepthFirst()
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.NONE );
    }

    @Test
    void testUnrestrictedTraversalCanFinishBreadthFirst()
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.NONE );
    }

    @Test
    void testNodeGlobalTraversalCanFinishDepthFirst()
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    void testNodeGlobalTraversalCanFinishBreadthFirst()
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    void testRelationshipGlobalTraversalCanFinishDepthFirst()
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    void testRelationshipGlobalTraversalCanFinishBreadthFirst()
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    void testNodePathTraversalCanFinishDepthFirst()
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    void testNodePathTraversalCanFinishBreadthFirst()
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    void testRelationshipPathTraversalCanFinishDepthFirst()
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    void testRelationshipPathTraversalCanFinishBreadthFirst()
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    void testNodeRecentTraversalCanFinishDepthFirst()
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    void testNodeRecentTraversalCanFinishBreadthFirst()
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    void testRelationshipRecentTraversalCanFinishDepthFirst()
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    @Test
    void testRelationshipRecentTraversalCanFinishBreadthFirst()
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    private void execute( TraversalDescription traversal, Uniqueness uniqueness )
    {
        try ( Transaction transaction = beginTx() )
        {
            Traverser traverser = traversal.uniqueness( uniqueness ).traverse( node( "1" ) );
            assertNotEquals( 0, Iterables.count( traverser ), "empty traversal" );
        }
    }

    @Test
    void testTraverseRelationshipsWithStartNodeNotIncluded()
    {
        try ( Transaction transaction = beginTx() )
        {
            TraversalDescription traversal = getGraphDb().traversalDescription().evaluator( excludeStartPosition() );
            assertEquals( 1, Iterables.count( traversal.traverse( node( "1" ) ).relationships() ) );
        }
    }
}
