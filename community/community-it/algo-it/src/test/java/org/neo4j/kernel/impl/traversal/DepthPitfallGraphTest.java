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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.graphdb.traversal.Evaluators.atDepth;
import static org.neo4j.graphdb.traversal.Evaluators.toDepth;

class DepthPitfallGraphTest extends TraversalTestBase
{
    /* Layout:
     *    _(2)--__
     *   /        \
     * (1)-(3)-_   (6)
     *  |\_     \  /
     *  |  (4)__ \/
     *  \_______(5)
     */
    private static final String[] THE_WORLD_AS_WE_KNOW_IT = {
            "1 TO 2", "1 TO 3", "1 TO 4", "5 TO 3", "1 TO 5", "4 TO 5",
            "2 TO 6", "5 TO 6" };
    private static final String[] NODE_UNIQUE_PATHS = { "1",
            "1,2", "1,2,6", "1,2,6,5", "1,2,6,5,3", "1,2,6,5,4", "1,3",
            "1,3,5", "1,3,5,4", "1,3,5,6", "1,3,5,6,2", "1,4", "1,4,5",
            "1,4,5,3", "1,4,5,6", "1,4,5,6,2", "1,5", "1,5,3", "1,5,4",
            "1,5,6", "1,5,6,2" };
    private static final String[] RELATIONSHIP_UNIQUE_EXTRA_PATHS = {
            "1,2,6,5,1", "1,2,6,5,1,3", "1,2,6,5,1,3,5", "1,2,6,5,1,3,5,4",
            "1,2,6,5,1,3,5,4,1", "1,2,6,5,1,4", "1,2,6,5,1,4,5",
            "1,2,6,5,1,4,5,3", "1,2,6,5,1,4,5,3,1", "1,2,6,5,3,1",
            "1,2,6,5,3,1,4", "1,2,6,5,3,1,4,5", "1,2,6,5,3,1,4,5,1",
            "1,2,6,5,3,1,5", "1,2,6,5,3,1,5,4", "1,2,6,5,3,1,5,4,1",
            "1,2,6,5,4,1", "1,2,6,5,4,1,3", "1,2,6,5,4,1,3,5",
            "1,2,6,5,4,1,3,5,1", "1,2,6,5,4,1,5", "1,2,6,5,4,1,5,3",
            "1,2,6,5,4,1,5,3,1", "1,3,5,1", "1,3,5,1,2", "1,3,5,1,2,6",
            "1,3,5,1,2,6,5", "1,3,5,1,2,6,5,4", "1,3,5,1,2,6,5,4,1",
            "1,3,5,1,4", "1,3,5,1,4,5", "1,3,5,1,4,5,6", "1,3,5,1,4,5,6,2",
            "1,3,5,1,4,5,6,2,1", "1,3,5,4,1", "1,3,5,4,1,2", "1,3,5,4,1,2,6",
            "1,3,5,4,1,2,6,5", "1,3,5,4,1,2,6,5,1", "1,3,5,4,1,5",
            "1,3,5,4,1,5,6", "1,3,5,4,1,5,6,2", "1,3,5,4,1,5,6,2,1",
            "1,3,5,6,2,1", "1,3,5,6,2,1,4", "1,3,5,6,2,1,4,5",
            "1,3,5,6,2,1,4,5,1", "1,3,5,6,2,1,5", "1,3,5,6,2,1,5,4",
            "1,3,5,6,2,1,5,4,1", "1,4,5,1", "1,4,5,1,2", "1,4,5,1,2,6",
            "1,4,5,1,2,6,5", "1,4,5,1,2,6,5,3", "1,4,5,1,2,6,5,3,1",
            "1,4,5,1,3", "1,4,5,1,3,5", "1,4,5,1,3,5,6", "1,4,5,1,3,5,6,2",
            "1,4,5,1,3,5,6,2,1", "1,4,5,3,1", "1,4,5,3,1,2", "1,4,5,3,1,2,6",
            "1,4,5,3,1,2,6,5", "1,4,5,3,1,2,6,5,1", "1,4,5,3,1,5",
            "1,4,5,3,1,5,6", "1,4,5,3,1,5,6,2", "1,4,5,3,1,5,6,2,1",
            "1,4,5,6,2,1", "1,4,5,6,2,1,3", "1,4,5,6,2,1,3,5",
            "1,4,5,6,2,1,3,5,1", "1,4,5,6,2,1,5", "1,4,5,6,2,1,5,3",
            "1,4,5,6,2,1,5,3,1", "1,5,3,1", "1,5,3,1,2", "1,5,3,1,2,6",
            "1,5,3,1,2,6,5", "1,5,3,1,2,6,5,4", "1,5,3,1,2,6,5,4,1",
            "1,5,3,1,4", "1,5,3,1,4,5", "1,5,3,1,4,5,6", "1,5,3,1,4,5,6,2",
            "1,5,3,1,4,5,6,2,1", "1,5,4,1", "1,5,4,1,2", "1,5,4,1,2,6",
            "1,5,4,1,2,6,5", "1,5,4,1,2,6,5,3", "1,5,4,1,2,6,5,3,1",
            "1,5,4,1,3", "1,5,4,1,3,5", "1,5,4,1,3,5,6", "1,5,4,1,3,5,6,2",
            "1,5,4,1,3,5,6,2,1", "1,5,6,2,1", "1,5,6,2,1,3", "1,5,6,2,1,3,5",
            "1,5,6,2,1,3,5,4", "1,5,6,2,1,3,5,4,1", "1,5,6,2,1,4",
            "1,5,6,2,1,4,5", "1,5,6,2,1,4,5,3", "1,5,6,2,1,4,5,3,1" };

    @BeforeEach
    void setup()
    {
        createGraph( THE_WORLD_AS_WE_KNOW_IT );
    }

    @Test
    void testSmallestPossibleInit()
    {
        int count = 0;
        try ( Transaction transaction = beginTx() )
        {
            Traverser traversal = transaction.traversalDescription()
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );
            for ( Path position : traversal )
            {
                count++;
                assertNotNull( position );
                assertNotNull( position.endNode() );
                if ( position.length() > 0 )
                {
                    assertNotNull( position.lastRelationship() );
                }
            }
            assertNotEquals( 0, count, "empty traversal" );
            transaction.commit();
        }
    }

    @Test
    void testAllNodesAreReturnedOnceDepthFirst()
    {
        testAllNodesAreReturnedOnce( transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void testAllNodesAreReturnedOnceBreadthFirst()
    {
        testAllNodesAreReturnedOnce( transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void testAllNodesAreReturnedOnce( Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( Uniqueness.NODE_GLOBAL )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );

            expectNodes( traverser, "1", "2", "3", "4", "5", "6" );
            transaction.commit();
        }
    }

    @Test
    void testNodesAreReturnedOnceWhenSufficientRecentlyUniqueDepthFirst()
    {
        testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
                transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void testNodesAreReturnedOnceWhenSufficientRecentlyUniqueBreadthFirst()
    {
        testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
                transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void testNodesAreReturnedOnceWhenSufficientRecentlyUnique( Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( Uniqueness.NODE_RECENT, 6 )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );

            expectNodes( traverser, "1", "2", "3", "4", "5", "6" );
            transaction.commit();
        }
    }

    @Test
    void testAllRelationshipsAreReturnedOnceDepthFirst()
    {
        testAllRelationshipsAreReturnedOnce( transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void testAllRelationshipsAreReturnedOnceBreadthFirst()
    {
        testAllRelationshipsAreReturnedOnce( transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void testAllRelationshipsAreReturnedOnce( Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( Uniqueness.RELATIONSHIP_GLOBAL )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );

            expectRelationships( traverser, THE_WORLD_AS_WE_KNOW_IT );
            transaction.commit();
        }
    }

    @Test
    void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUniqueDepthFirst()
    {
        testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
                transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUniqueBreadthFirst()
    {
        testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
                transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
            Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( Uniqueness.RELATIONSHIP_RECENT, THE_WORLD_AS_WE_KNOW_IT.length )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );

            expectRelationships( traverser, THE_WORLD_AS_WE_KNOW_IT );
            transaction.commit();
        }
    }

    @Test
    void testAllUniqueNodePathsAreReturnedDepthFirst()
    {
        testAllUniqueNodePathsAreReturned( transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void testAllUniqueNodePathsAreReturnedBreadthFirst()
    {
        testAllUniqueNodePathsAreReturned( transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void testAllUniqueNodePathsAreReturned( Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( Uniqueness.NODE_PATH )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );

            expectPaths( traverser, NODE_UNIQUE_PATHS );
            transaction.commit();
        }
    }

    @Test
    void testAllUniqueRelationshipPathsAreReturnedDepthFirst()
    {
        testAllUniqueRelationshipPathsAreReturned( transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void testAllUniqueRelationshipPathsAreReturnedBreadthFirst()
    {
        testAllUniqueRelationshipPathsAreReturned( transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void testAllUniqueRelationshipPathsAreReturned( Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Set<String> expected = new HashSet<>( Arrays.asList( NODE_UNIQUE_PATHS ) );
            expected.addAll( Arrays.asList( RELATIONSHIP_UNIQUE_EXTRA_PATHS ) );

            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( Uniqueness.RELATIONSHIP_PATH )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );

            expectPaths( traverser, expected );
            transaction.commit();
        }
    }

    @Test
    void canPruneTraversalAtSpecificDepthDepthFirst()
    {
        canPruneTraversalAtSpecificDepth( transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void canPruneTraversalAtSpecificDepthBreadthFirst()
    {
        canPruneTraversalAtSpecificDepth( transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void canPruneTraversalAtSpecificDepth( Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( Uniqueness.NONE )
                    .evaluator( toDepth( 1 ) )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );

            expectNodes( traverser, "1", "2", "3", "4", "5" );
            transaction.commit();
        }
    }

    @Test
    void canPreFilterNodesDepthFirst()
    {
        canPreFilterNodes( transaction -> transaction.traversalDescription().depthFirst() );
    }

    @Test
    void canPreFilterNodesBreadthFirst()
    {
        canPreFilterNodes( transaction -> transaction.traversalDescription().breadthFirst() );
    }

    private void canPreFilterNodes( Function<Transaction,TraversalDescription> traversalFactory )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = traversalFactory.apply( transaction )
                    .uniqueness( Uniqueness.NONE )
                    .evaluator( atDepth( 2 ) )
                    .traverse( transaction.getNodeById( node( "1" ).getId() ) );

            expectPaths( traverser, "1,2,6", "1,3,5", "1,4,5", "1,5,3", "1,5,4", "1,5,6" );
            transaction.commit();
        }
    }
}
