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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.BidirectionalTraversalDescription;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.kernel.Uniqueness;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.neo4j.graphdb.traversal.Evaluators.atDepth;
import static org.neo4j.helpers.collection.IteratorUtil.first;
import static org.neo4j.kernel.Traversal.bidirectionalTraversal;
import static org.neo4j.kernel.Traversal.traversal;

public class TestPath extends TraversalTestBase
{
    private static Node a,b,c,d,e;
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
        Path path = traversal().evaluator( atDepth( 4 ) ).traverse( node( "A" ) ).iterator().next();
        
        assertPathIsCorrect( path );
    }


    
    @Test
    public void reverseNodes() throws Exception
    {
        Path path = first( traversal().evaluator( atDepth( 0 ) ).traverse( a ) );
        assertContains( path.reverseNodes(), a );
        
        path = first( traversal().evaluator( atDepth( 4 ) ).traverse( a ) );
        assertContainsInOrder( path.reverseNodes(), e, d, c, b, a );
    }

    @Test
    public void reverseRelationships() throws Exception
    {
        Path path = first( traversal().evaluator( atDepth( 0 ) ).traverse( a ) );
        assertFalse( path.reverseRelationships().iterator().hasNext() );
        
        path = first( traversal().evaluator( atDepth( 4 ) ).traverse( a ) );
        Node[] expectedNodes = new Node[] { e, d, c, b, a };
        int index = 0;
        for ( Relationship rel : path.reverseRelationships() )
            assertEquals( "For index " + index, expectedNodes[index++], rel.getEndNode() );
        assertEquals( 4, index );
    }
    
    @Test
    public void testBidirectionalPath() throws Exception
    {
        TraversalDescription side = traversal().uniqueness( Uniqueness.NODE_PATH );
        BidirectionalTraversalDescription bidirectional = bidirectionalTraversal().mirroredSides( side );
        Path bidirectionalPath = first( bidirectional.traverse( a, e ) );
        assertPathIsCorrect( bidirectionalPath );
        
        assertEquals( a, first( bidirectional.traverse( a, e ) ).startNode() );
        
        // White box testing below: relationships(), nodes(), reverseRelationships(), reverseNodes()
        // does cache the start node if not already cached, so just make sure they to it properly.
        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.relationships();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.nodes();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.reverseRelationships();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.reverseNodes();
        assertEquals( a, bidirectionalPath.startNode() );

        bidirectionalPath = first( bidirectional.traverse( a, e ) );
        bidirectionalPath.iterator();
        assertEquals( a, bidirectionalPath.startNode() );
    }

    private void assertPathIsCorrect( Path path )
    {
        Node a = node( "A" );
        Relationship to1 = a.getRelationships( Direction.OUTGOING ).iterator().next();
        Node b = to1.getEndNode();
        Relationship to2 = b.getRelationships( Direction.OUTGOING ).iterator().next();
        Node c = to2.getEndNode();
        Relationship to3 = c.getRelationships( Direction.OUTGOING ).iterator().next();
        Node d = to3.getEndNode();
        Relationship to4 = d.getRelationships( Direction.OUTGOING ).iterator().next();
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
}
