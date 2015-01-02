/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Traverser;

import static java.util.Arrays.asList;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import static org.neo4j.kernel.Traversal.postorderBreadthFirst;
import static org.neo4j.kernel.Traversal.postorderDepthFirst;
import static org.neo4j.kernel.Traversal.traversal;

public class TreeGraphTest extends TraversalTestBase
{
    /*
     *                     (1)
     *               ------ | ------
     *             /        |        \
     *           (2)       (3)       (4)
     *          / | \     / | \     / | \
     *        (5)(6)(7) (8)(9)(A) (B)(C)(D)
     */
    private static final String[] THE_WORLD_AS_WE_KNOWS_IT = new String[] {
            "1 TO 2", "1 TO 3", "1 TO 4", "2 TO 5", "2 TO 6", "2 TO 7",
            "3 TO 8", "3 TO 9", "3 TO A", "4 TO B", "4 TO C", "4 TO D", };

    @Before
    public void setupGraph()
    {
        createGraph( THE_WORLD_AS_WE_KNOWS_IT );
    }

    @Test
    public void nodesIteratorReturnAllNodes() throws Exception
    {
        Transaction transaction = beginTx();
        try
        {
            Traverser traverser = traversal().traverse( node( "1" ) );
            int count = 0;
            for ( Node node : traverser.nodes() )
            {
                assertNotNull( "returned nodes should not be null. node #"
                               + count, node );
                count++;
            }
            assertEquals( 13, count );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void relationshipsIteratorReturnAllNodes() throws Exception
    {
        Transaction transaction = beginTx();
        try
        {
            Traverser traverser = traversal().traverse( node( "1" ) );
            int count = 0;
            for ( Relationship relationship : traverser.relationships() )
            {
                assertNotNull(
                        "returned relationships should not be. relationship #"
                                + count, relationship );
                count++;
            }
            assertEquals( 12, count );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void pathsIteratorReturnAllNodes() throws Exception
    {
        Transaction transaction = beginTx();
        try
        {
            Traverser traverser = traversal().traverse( node( "1" ) );
            int count = 0;
            for ( Path path : traverser )
            {
                assertNotNull( "returned paths should not be null. path #"
                               + count, path );
                count++;
            }
            assertEquals( 13, count );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void testBreadthFirst() throws Exception
    {
        Traverser traverser = traversal().breadthFirst().traverse( node( "1" ) );
        Stack<Set<String>> levels = new Stack<Set<String>>();
        levels.push( new HashSet<String>( asList( "5", "6", "7", "8",
                "9", "A", "B", "C", "D" ) ) );
        levels.push( new HashSet<String>( asList( "2", "3", "4" ) ) );
        levels.push( new HashSet<String>( asList( "1" ) ) );

        Transaction tx = beginTx();
        try
        {
            assertLevels( traverser, levels );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void testDepthFirstTraversalReturnsNodesOnCorrectDepths()
            throws Exception
    {
        Transaction transaction = beginTx();
        try
        {
            Traverser traverser = traversal().depthFirst().traverse( node( "1" ) );
            int i = 0;
            for ( Path pos : traverser )
            {
                assertEquals( expectedDepth( i++ ), pos.length() );
            }
            assertEquals( 13, i );
        }
        finally
        {
            transaction.finish();
        }
    }

    @Test
    public void testPostorderDepthFirstReturnsDeeperNodesFirst()
    {
        Traverser traverser = traversal().order( postorderDepthFirst() ).traverse( node( "1" ) );
        int i = 0;
        List<String> encounteredNodes = new ArrayList<String>();
        Transaction tx = beginTx();
        for ( Path pos : traverser )
        {
            encounteredNodes.add( (String) pos.endNode().getProperty( "name" ) );
            assertEquals( expectedDepth( ( 12 - i++ ) ), pos.length() );
        }
        tx.success();
        tx.finish();
        assertEquals( 13, i );

        assertTrue( encounteredNodes.indexOf( "5" ) < encounteredNodes.indexOf( "2" ) );
        assertTrue( encounteredNodes.indexOf( "6" ) < encounteredNodes.indexOf( "2" ) );
        assertTrue( encounteredNodes.indexOf( "7" ) < encounteredNodes.indexOf( "2" ) );
        assertTrue( encounteredNodes.indexOf( "8" ) < encounteredNodes.indexOf( "3" ) );
        assertTrue( encounteredNodes.indexOf( "9" ) < encounteredNodes.indexOf( "3" ) );
        assertTrue( encounteredNodes.indexOf( "A" ) < encounteredNodes.indexOf( "3" ) );
        assertTrue( encounteredNodes.indexOf( "B" ) < encounteredNodes.indexOf( "4" ) );
        assertTrue( encounteredNodes.indexOf( "C" ) < encounteredNodes.indexOf( "4" ) );
        assertTrue( encounteredNodes.indexOf( "D" ) < encounteredNodes.indexOf( "4" ) );
        assertTrue( encounteredNodes.indexOf( "2" ) < encounteredNodes.indexOf( "1" ) );
        assertTrue( encounteredNodes.indexOf( "3" ) < encounteredNodes.indexOf( "1" ) );
        assertTrue( encounteredNodes.indexOf( "4" ) < encounteredNodes.indexOf( "1" ) );
    }

    @Test
    public void testPostorderBreadthFirstReturnsDeeperNodesFirst()
    {
        Traverser traverser = traversal().order( postorderBreadthFirst() ).traverse( node( "1" ) );
        Stack<Set<String>> levels = new Stack<Set<String>>();
        levels.push( new HashSet<String>( asList( "1" ) ) );
        levels.push( new HashSet<String>( asList( "2", "3", "4" ) ) );
        levels.push( new HashSet<String>( asList( "5", "6", "7", "8",
                "9", "A", "B", "C", "D" ) ) );
        Transaction tx = beginTx();
        try
        {
            assertLevels( traverser, levels );
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }

    private int expectedDepth( int i )
    {
        assertTrue( i < 13 );
        if ( i == 0 )
        {
            return 0;
        }
        else if ( ( i - 1 ) % 4 == 0 )
        {
            return 1;
        }
        else
        {
            return 2;
        }
    }
}
