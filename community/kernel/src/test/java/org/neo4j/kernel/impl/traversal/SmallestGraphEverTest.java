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

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.collection.Iterables;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.traversal.Evaluators.excludeStartPosition;

public class SmallestGraphEverTest extends TraversalTestBase
{
    @Before
    public void setup()
    {
        createGraph( "1 TO 2" );
    }

    @Test
    public void testUnrestrictedTraversalCanFinishDepthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.NONE );
    }

    @Test
    public void testUnrestrictedTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.NONE );
    }

    @Test
    public void testNodeGlobalTraversalCanFinishDepthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    public void testNodeGlobalTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    public void testRelationshipGlobalTraversalCanFinishDepthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    public void testRelationshipGlobalTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    public void testNodePathTraversalCanFinishDepthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    public void testNodePathTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    public void testRelationshipPathTraversalCanFinishDepthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    public void testRelationshipPathTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    public void testNodeRecentTraversalCanFinishDepthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    public void testNodeRecentTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    public void testRelationshipRecentTraversalCanFinishDepthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().depthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    @Test
    public void testRelationshipRecentTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( getGraphDb().traversalDescription().breadthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    private void execute( TraversalDescription traversal, Uniqueness uniqueness )
    {
        try ( Transaction transaction = beginTx() )
        {
            Traverser traverser = traversal.uniqueness( uniqueness ).traverse( node( "1" ) );
            assertFalse( "empty traversal", Iterables.count( traverser ) == 0 );
        }
    }

    @Test
    public void testTraverseRelationshipsWithStartNodeNotIncluded() throws Exception
    {
        try ( Transaction transaction = beginTx() )
        {
            TraversalDescription traversal = getGraphDb().traversalDescription().evaluator( excludeStartPosition() );
            assertEquals( 1, Iterables.count( traversal.traverse( node( "1" ) ).relationships() ) );
        }
    }
}
