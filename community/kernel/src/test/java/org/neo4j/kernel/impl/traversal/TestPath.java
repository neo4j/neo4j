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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.helpers.collection.Iterators;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.traversal.Evaluators.atDepth;

public class TestPath extends TraversalTestBase
{
    private static Node a;
    private static Node b;
    private static Node c;
    private static Node d;
    private static Node e;
    private Transaction tx;

    @Before
    public void setup()
    {
        createGraph( "A TO B", "B TO C", "C TO D", "D TO E" );

        tx = beginTx();

        a = getNodeWithName( "A" );
        b = getNodeWithName( "B" );
        c = getNodeWithName( "C" );
        d = getNodeWithName( "D" );
        e = getNodeWithName( "E" );
    }

    @After
    public void tearDown()
    {
        tx.close();
    }

    @Test
    public void testPathIterator()
    {
        Traverser traverse = getGraphDb().traversalDescription().evaluator( atDepth( 4 ) ).traverse( node( "A" ) );
        try ( ResourceIterator<Path> resourceIterator = traverse.iterator() )
        {
            Path path = resourceIterator.next();
            assertPathIsCorrect( path );
        }
    }

    @Test
    public void reverseNodes()
    {
        Traverser traverse = getGraphDb().traversalDescription().evaluator( atDepth( 0 ) ).traverse( a );
        Path path = getFirstPath( traverse );
        assertContains( path.reverseNodes(), a );

        Traverser traverse2 = getGraphDb().traversalDescription().evaluator( atDepth( 4 ) ).traverse( a );
        Path path2 = getFirstPath( traverse2 );
        assertContainsInOrder( path2.reverseNodes(), e, d, c, b, a );
    }

    @Test
    public void reverseRelationships()
    {
        Traverser traverser = getGraphDb().traversalDescription().evaluator( atDepth( 0 ) ).traverse( a );
        Path path = getFirstPath( traverser );
        assertFalse( path.reverseRelationships().iterator().hasNext() );

        Traverser traverser2 = getGraphDb().traversalDescription().evaluator( atDepth( 4 ) ).traverse( a );
        Path path2 = getFirstPath( traverser2 );
        Node[] expectedNodes = new Node[]{e, d, c, b, a};
        int index = 0;
        for ( Relationship rel : path2.reverseRelationships() )
        {
            assertEquals( "For index " + index, expectedNodes[index++], rel.getEndNode() );
        }
        assertEquals( 4, index );
    }

    @Test
    public void testBidirectionalPath()
    {
        TraversalDescription side = getGraphDb().traversalDescription().uniqueness( Uniqueness.NODE_PATH );
        BidirectionalTraversalDescription bidirectional =
                getGraphDb().bidirectionalTraversalDescription().mirroredSides( side );
        Path bidirectionalPath = getFirstPath( bidirectional.traverse( a, e ) );
        assertPathIsCorrect( bidirectionalPath );

        Path path = getFirstPath( bidirectional.traverse( a, e ) );
        Node node = path.startNode();
        assertEquals( a, node );

        // White box testing below: relationships(), nodes(), reverseRelationships(), reverseNodes()
        // does cache the start node if not already cached, so just make sure they to it properly.
        bidirectionalPath = getFirstPath( bidirectional.traverse( a, e ) );
        bidirectionalPath.relationships();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = getFirstPath(bidirectional.traverse(a,e ) );
        bidirectionalPath.nodes();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = getFirstPath( bidirectional.traverse( a, e ) );
        bidirectionalPath.reverseRelationships();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = getFirstPath( bidirectional.traverse( a, e ) );
        bidirectionalPath.reverseNodes();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = getFirstPath( bidirectional.traverse( a, e ) );
        bidirectionalPath.iterator();
        assertEquals( a, bidirectionalPath.startNode() );
    }

    private Path getFirstPath( Traverser traverse )
    {
        try ( ResourceIterator<Path> iterator = traverse.iterator() )
        {
            return Iterators.first( iterator );
        }
    }

    private void assertPathIsCorrect( Path path )
    {
        Node a = node( "A" );
        Relationship to1 = getFistRelationship( a );
        Node b = to1.getEndNode();
        Relationship to2 = getFistRelationship( b );
        Node c = to2.getEndNode();
        Relationship to3 = getFistRelationship( c );
        Node d = to3.getEndNode();
        Relationship to4 = getFistRelationship( d );
        Node e = to4.getEndNode();
        assertEquals( (Integer) 4, (Integer) path.length() );
        assertEquals( a, path.startNode() );
        assertEquals( e, path.endNode() );
        assertEquals( to4, path.lastRelationship() );

        assertContainsInOrder( path, a, to1, b, to2, c, to3, d, to4, e );
        assertContainsInOrder( path.nodes(), a, b, c, d, e );
        assertContainsInOrder( path.relationships(), to1, to2, to3, to4 );
        assertContainsInOrder( path.reverseNodes(), e, d, c, b, a );
        assertContainsInOrder( path.reverseRelationships(), to4, to3, to2, to1 );
    }

    private Relationship getFistRelationship( Node node )
    {
        ResourceIterable<Relationship> relationships = (ResourceIterable<Relationship>) node.getRelationships( Direction.OUTGOING );
        try ( ResourceIterator<Relationship> iterator = relationships.iterator() )
        {
            return iterator.next();
        }
    }
}
