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
        execute( tx -> tx.traversalDescription().depthFirst(), Uniqueness.NONE );
    }

    @Test
    void testUnrestrictedTraversalCanFinishBreadthFirst()
    {
        execute( tx -> tx.traversalDescription().breadthFirst(), Uniqueness.NONE );
    }

    @Test
    void testNodeGlobalTraversalCanFinishDepthFirst()
    {
        execute( tx -> tx.traversalDescription().depthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    void testNodeGlobalTraversalCanFinishBreadthFirst()
    {
        execute( tx -> tx.traversalDescription().breadthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    void testRelationshipGlobalTraversalCanFinishDepthFirst()
    {
        execute( tx -> tx.traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    void testRelationshipGlobalTraversalCanFinishBreadthFirst()
    {
        execute( tx -> tx.traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    void testNodePathTraversalCanFinishDepthFirst()
    {
        execute( tx -> tx.traversalDescription().depthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    void testNodePathTraversalCanFinishBreadthFirst()
    {
        execute( tx -> tx.traversalDescription().breadthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    void testRelationshipPathTraversalCanFinishDepthFirst()
    {
        execute( tx -> tx.traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    void testRelationshipPathTraversalCanFinishBreadthFirst()
    {
        execute( tx -> tx.traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    void testNodeRecentTraversalCanFinishDepthFirst()
    {
        execute( tx -> tx.traversalDescription().depthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    void testNodeRecentTraversalCanFinishBreadthFirst()
    {
        execute( tx -> tx.traversalDescription().breadthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    void testRelationshipRecentTraversalCanFinishDepthFirst()
    {
        execute( tx -> tx.traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    @Test
    void testRelationshipRecentTraversalCanFinishBreadthFirst()
    {
        execute( tx -> tx.traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    private void execute( Function<Transaction,TraversalDescription> traversalFactory, Uniqueness uniqueness )
    {
        try ( Transaction transaction = beginTx() )
        {
            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( uniqueness )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );
            assertNotEquals( 0, Iterables.count( traverser ), "empty traversal" );
        }
    }

    @Test
    void testTraverseRelationshipsWithStartNodeNotIncluded()
    {
        try ( Transaction transaction = beginTx() )
        {
            TraversalDescription traversal = transaction.traversalDescription().evaluator( excludeStartPosition() );
            assertEquals( 1, Iterables.count( traversal
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) ).relationships() ) );
        }
    }
}
