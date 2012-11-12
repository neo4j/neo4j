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
import static org.junit.Assert.assertFalse;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.Traversal;
import org.neo4j.kernel.Uniqueness;

public class SmallestGraphEverTest extends AbstractTestBase
{
    @BeforeClass
    public static void setup()
    {
        createGraph( "1 TO 2" );
    }

    @Test
    public void testUnrestrictedTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.NONE );
    }

    @Test
    public void testUnrestrictedTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.NONE );
    }

    @Test
    public void testNodeGlobalTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    public void testNodeGlobalTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.NODE_GLOBAL );
    }

    @Test
    public void testRelationshipGlobalTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    public void testRelationshipGlobalTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.RELATIONSHIP_GLOBAL );
    }

    @Test
    public void testNodePathTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    public void testNodePathTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.NODE_PATH );
    }

    @Test
    public void testRelationshipPathTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    public void testRelationshipPathTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.RELATIONSHIP_PATH );
    }

    @Test
    public void testNodeRecentTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    public void testNodeRecentTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.NODE_RECENT );
    }

    @Test
    public void testRelationshipRecentTraversalCanFinishDepthFirst() throws Exception
    {
        execute( Traversal.description().depthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    @Test
    public void testRelationshipRecentTraversalCanFinishBreadthFirst() throws Exception
    {
        execute( Traversal.description().breadthFirst(), Uniqueness.RELATIONSHIP_RECENT );
    }

    private void execute( TraversalDescription traversal, Uniqueness uniqueness )
    {
        Traverser traverser = traversal.uniqueness( uniqueness ).traverse(
                node( "1" ) );
        assertFalse( "empty traversal", IteratorUtil.count( traverser ) == 0 );
    }

    @Test
    @SuppressWarnings( "deprecation" )
    public void testTraverseRelationshipsWithStartNodeNotIncluded() throws Exception
    {
        TraversalDescription traversal = Traversal.description().filter(
                Traversal.returnAllButStartNode() );
        int count = 0;
        for ( Relationship rel : traversal.traverse( node( "1" ) ).relationships() )
        {
            count++;
        }
        assertEquals( 1, count );
    }
}
