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
        Traverser traversal = getGraphDb().traversalDescription().traverse( node( "1" ) );
        int count = 0;
        try ( Transaction transaction = beginTx() )
        {
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
        testAllNodesAreReturnedOnce( getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void testAllNodesAreReturnedOnceBreadthFirst()
    {
        testAllNodesAreReturnedOnce( getGraphDb().traversalDescription().breadthFirst() );
    }

    private void testAllNodesAreReturnedOnce( TraversalDescription traversal )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = traversal.uniqueness( Uniqueness.NODE_GLOBAL ).traverse( node( "1" ) );

            expectNodes( traverser, "1", "2", "3", "4", "5", "6" );
            transaction.commit();
        }
    }

    @Test
    void testNodesAreReturnedOnceWhenSufficientRecentlyUniqueDepthFirst()
    {
        testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
                getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void testNodesAreReturnedOnceWhenSufficientRecentlyUniqueBreadthFirst()
    {
        testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
                getGraphDb().traversalDescription().breadthFirst() );
    }

    private void testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
            TraversalDescription description )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = description.uniqueness( Uniqueness.NODE_RECENT, 6 ).traverse( node( "1" ) );

            expectNodes( traverser, "1", "2", "3", "4", "5", "6" );
            transaction.commit();
        }
    }

    @Test
    void testAllRelationshipsAreReturnedOnceDepthFirst()
    {
        testAllRelationshipsAreReturnedOnce( getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void testAllRelationshipsAreReturnedOnceBreadthFirst()
    {
        testAllRelationshipsAreReturnedOnce( getGraphDb().traversalDescription().breadthFirst() );
    }

    private void testAllRelationshipsAreReturnedOnce(
            TraversalDescription description )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = getGraphDb().traversalDescription().uniqueness( Uniqueness.RELATIONSHIP_GLOBAL ).traverse( node( "1" ) );

            expectRelationships( traverser, THE_WORLD_AS_WE_KNOW_IT );
            transaction.commit();
        }
    }

    @Test
    void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUniqueDepthFirst()
    {
        testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
                getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUniqueBreadthFirst()
    {
        testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
                getGraphDb().traversalDescription().breadthFirst() );
    }

    private void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
            TraversalDescription description )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = description.uniqueness( Uniqueness.RELATIONSHIP_RECENT, THE_WORLD_AS_WE_KNOW_IT.length ).traverse( node( "1" ) );

            expectRelationships( traverser, THE_WORLD_AS_WE_KNOW_IT );
            transaction.commit();
        }
    }

    @Test
    void testAllUniqueNodePathsAreReturnedDepthFirst()
    {
        testAllUniqueNodePathsAreReturned( getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void testAllUniqueNodePathsAreReturnedBreadthFirst()
    {
        testAllUniqueNodePathsAreReturned( getGraphDb().traversalDescription().breadthFirst() );
    }

    private void testAllUniqueNodePathsAreReturned( TraversalDescription description )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = description.uniqueness( Uniqueness.NODE_PATH ).traverse( node( "1" ) );

            expectPaths( traverser, NODE_UNIQUE_PATHS );
            transaction.commit();
        }
    }

    @Test
    void testAllUniqueRelationshipPathsAreReturnedDepthFirst()
    {
        testAllUniqueRelationshipPathsAreReturned( getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void testAllUniqueRelationshipPathsAreReturnedBreadthFirst()
    {
        testAllUniqueRelationshipPathsAreReturned( getGraphDb().traversalDescription().breadthFirst() );
    }

    private void testAllUniqueRelationshipPathsAreReturned( TraversalDescription description )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Set<String> expected = new HashSet<>( Arrays.asList( NODE_UNIQUE_PATHS ) );
            expected.addAll( Arrays.asList( RELATIONSHIP_UNIQUE_EXTRA_PATHS ) );

            Traverser traverser = description.uniqueness( Uniqueness.RELATIONSHIP_PATH ).traverse( node( "1" ) );

            expectPaths( traverser, expected );
            transaction.commit();
        }
    }

    @Test
    void canPruneTraversalAtSpecificDepthDepthFirst()
    {
        canPruneTraversalAtSpecificDepth( getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void canPruneTraversalAtSpecificDepthBreadthFirst()
    {
        canPruneTraversalAtSpecificDepth( getGraphDb().traversalDescription().breadthFirst() );
    }

    private void canPruneTraversalAtSpecificDepth( TraversalDescription description )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = description.uniqueness( Uniqueness.NONE ).evaluator( toDepth( 1 ) ).traverse( node( "1" ) );

            expectNodes( traverser, "1", "2", "3", "4", "5" );
            transaction.commit();
        }
    }

    @Test
    void canPreFilterNodesDepthFirst()
    {
        canPreFilterNodes( getGraphDb().traversalDescription().depthFirst() );
    }

    @Test
    void canPreFilterNodesBreadthFirst()
    {
        canPreFilterNodes( getGraphDb().traversalDescription().breadthFirst() );
    }

    private void canPreFilterNodes( TraversalDescription description )
    {
        try ( Transaction transaction = getGraphDb().beginTx() )
        {
            Traverser traverser = description.uniqueness( Uniqueness.NONE ).evaluator( atDepth( 2 ) ).traverse( node( "1" ) );

            expectPaths( traverser, "1,2,6", "1,3,5", "1,4,5", "1,5,3", "1,5,4", "1,5,6" );
            transaction.commit();
        }
    }
}
