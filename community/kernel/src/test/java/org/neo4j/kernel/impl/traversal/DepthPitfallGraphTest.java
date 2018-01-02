/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.graphdb.traversal.Evaluators.atDepth;
import static org.neo4j.graphdb.traversal.Evaluators.toDepth;
import static org.neo4j.kernel.Traversal.traversal;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Uniqueness;

public class DepthPitfallGraphTest extends TraversalTestBase
{
    /* Layout:
     *    _(2)--__
     *   /        \
     * (1)-(3)-_   (6)
     *  |\_     \  /
     *  |  (4)__ \/
     *  \_______(5)
     */
    private static final String[] THE_WORLD_AS_WE_KNOW_IT = new String[] {
            "1 TO 2", "1 TO 3", "1 TO 4", "5 TO 3", "1 TO 5", "4 TO 5",
            "2 TO 6", "5 TO 6" };
    private static final String[] NODE_UNIQUE_PATHS = new String[] { "1",
            "1,2", "1,2,6", "1,2,6,5", "1,2,6,5,3", "1,2,6,5,4", "1,3",
            "1,3,5", "1,3,5,4", "1,3,5,6", "1,3,5,6,2", "1,4", "1,4,5",
            "1,4,5,3", "1,4,5,6", "1,4,5,6,2", "1,5", "1,5,3", "1,5,4",
            "1,5,6", "1,5,6,2" };
    private static final String[] RELATIONSHIP_UNIQUE_EXTRA_PATHS = new String[] {
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

    @Before
    public void setup()
    {
        createGraph( THE_WORLD_AS_WE_KNOW_IT );
    }

    @Test
    public void testSmallestPossibleInit() throws Exception
    {
        Traverser traversal = traversal().traverse( node( "1" ) );
        int count = 0;
        try (Transaction transaction = beginTx())
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
                assertNotNull( position.length() );
            }
            assertFalse( "empty traversal", count == 0 );
        }
    }

    @Test
    public void testAllNodesAreReturnedOnceDepthFirst() throws Exception
    {
        testAllNodesAreReturnedOnce( traversal().depthFirst() );
    }

    @Test
    public void testAllNodesAreReturnedOnceBreadthFirst() throws Exception
    {
        testAllNodesAreReturnedOnce( traversal().breadthFirst() );
    }

    private void testAllNodesAreReturnedOnce( TraversalDescription traversal )
    {
        Traverser traverser = traversal.uniqueness( Uniqueness.NODE_GLOBAL ).traverse(
                node( "1" ) );

        expectNodes( traverser, "1", "2", "3", "4", "5", "6" );
    }

    @Test
    public void testNodesAreReturnedOnceWhenSufficientRecentlyUniqueDepthFirst()
            throws Exception
    {
        testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
                traversal().depthFirst() );
    }

    @Test
    public void testNodesAreReturnedOnceWhenSufficientRecentlyUniqueBreadthFirst()
            throws Exception
    {
        testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
                traversal().breadthFirst() );
    }

    private void testNodesAreReturnedOnceWhenSufficientRecentlyUnique(
            TraversalDescription description )
    {
        Traverser traverser = description.uniqueness( Uniqueness.NODE_RECENT, 6 ).traverse(
                node( "1" ) );

        expectNodes( traverser, "1", "2", "3", "4", "5", "6" );
    }

    @Test
    public void testAllRelationshipsAreReturnedOnceDepthFirst()
            throws Exception
    {
        testAllRelationshipsAreReturnedOnce( traversal().depthFirst() );
    }

    @Test
    public void testAllRelationshipsAreReturnedOnceBreadthFirst()
            throws Exception
    {
        testAllRelationshipsAreReturnedOnce( traversal().breadthFirst() );
    }

    private void testAllRelationshipsAreReturnedOnce(
            TraversalDescription description ) throws Exception
    {
        Traverser traverser = traversal().uniqueness(
                Uniqueness.RELATIONSHIP_GLOBAL ).traverse( node( "1" ) );

        expectRelationships( traverser, THE_WORLD_AS_WE_KNOW_IT );
    }

    @Test
    public void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUniqueDepthFirst()
            throws Exception
    {
        testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
                traversal().depthFirst() );
    }

    @Test
    public void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUniqueBreadthFirst()
            throws Exception
    {
        testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
                traversal().breadthFirst() );
    }

    private void testRelationshipsAreReturnedOnceWhenSufficientRecentlyUnique(
            TraversalDescription description ) throws Exception
    {
        Traverser traverser = description.uniqueness(
                Uniqueness.RELATIONSHIP_RECENT, THE_WORLD_AS_WE_KNOW_IT.length ).traverse(
                        node( "1" ) );

        expectRelationships( traverser, THE_WORLD_AS_WE_KNOW_IT );
    }

    @Test
    public void testAllUniqueNodePathsAreReturnedDepthFirst() throws Exception
    {
        testAllUniqueNodePathsAreReturned( traversal().depthFirst() );
    }

    @Test
    public void testAllUniqueNodePathsAreReturnedBreadthFirst() throws Exception
    {
        testAllUniqueNodePathsAreReturned( traversal().breadthFirst() );
    }

    private void testAllUniqueNodePathsAreReturned( TraversalDescription description )
            throws Exception
    {
        Traverser traverser = description.uniqueness(
                Uniqueness.NODE_PATH ).traverse( node( "1" ) );

        expectPaths( traverser, NODE_UNIQUE_PATHS );
    }

    @Test
    public void testAllUniqueRelationshipPathsAreReturnedDepthFirst() throws Exception
    {
        testAllUniqueRelationshipPathsAreReturned( traversal().depthFirst() );
    }

    @Test
    public void testAllUniqueRelationshipPathsAreReturnedBreadthFirst() throws Exception
    {
        testAllUniqueRelationshipPathsAreReturned( traversal().breadthFirst() );
    }

    private void testAllUniqueRelationshipPathsAreReturned( TraversalDescription description )
            throws Exception
    {
        Set<String> expected = new HashSet<String>(
                Arrays.asList( NODE_UNIQUE_PATHS ) );
        expected.addAll( Arrays.asList( RELATIONSHIP_UNIQUE_EXTRA_PATHS ) );

        Traverser traverser = description.uniqueness(
                Uniqueness.RELATIONSHIP_PATH ).traverse( node( "1" ) );

        expectPaths( traverser, expected );
    }

    @Test
    public void canPruneTraversalAtSpecificDepthDepthFirst()
    {
        canPruneTraversalAtSpecificDepth( traversal().depthFirst() );
    }

    @Test
    public void canPruneTraversalAtSpecificDepthBreadthFirst()
    {
        canPruneTraversalAtSpecificDepth( traversal().breadthFirst() );
    }

    private void canPruneTraversalAtSpecificDepth( TraversalDescription description )
    {
        Traverser traverser = description.uniqueness(
                Uniqueness.NONE ).evaluator( toDepth( 1 ) ).traverse( node( "1" ) );

        expectNodes( traverser, "1", "2", "3", "4", "5" );
    }

    @Test
    public void canPreFilterNodesDepthFirst()
    {
        canPreFilterNodes( traversal().depthFirst() );
    }

    @Test
    public void canPreFilterNodesBreadthFirst()
    {
        canPreFilterNodes( traversal().breadthFirst() );
    }

    private void canPreFilterNodes( TraversalDescription description )
    {
        Traverser traverser = description.uniqueness(
                Uniqueness.NONE ).evaluator( atDepth( 2 ) ).traverse( node( "1" ) );

        expectPaths( traverser, "1,2,6", "1,3,5", "1,4,5", "1,5,3", "1,5,4",
                "1,5,6" );
    }
}
