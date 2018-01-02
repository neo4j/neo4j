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
package org.neo4j.graphalgo.shortestpath;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;

import org.junit.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.impl.shortestpath.Dijkstra;
import org.neo4j.graphalgo.impl.util.DoubleAdder;
import org.neo4j.graphalgo.impl.util.DoubleComparator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import common.Neo4jAlgoTestCase;

public class DijkstraIteratorTest extends Neo4jAlgoTestCase
{
    @Test
    public void testRun()
    {
        new TestDijkstra().runTest();
    }

    protected class TestDijkstra extends Dijkstra<Double>
    {
        public TestDijkstra()
        {
            super( 0.0, null, null, CommonEvaluators.doubleCostEvaluator( "cost" ),
                new DoubleAdder(), new DoubleComparator(), Direction.BOTH,
                MyRelTypes.R1 );
        }

        protected class TestIterator extends Dijkstra<Double>.DijstraIterator
        {
            public TestIterator( Node startNode,
                HashMap<Node,List<Relationship>> predecessors,
                HashMap<Node,Double> mySeen, HashMap<Node,Double> otherSeen,
                HashMap<Node,Double> myDistances,
                HashMap<Node,Double> otherDistances, boolean backwards )
            {
                super( startNode, predecessors, mySeen, otherSeen, myDistances,
                    otherDistances, backwards );
            }
        }

        @Test
        public void runTest()
        {
            graph.makeEdge( "start", "a", "cost", (double) 1 );
            graph.makeEdge( "a", "x", "cost", (double) 9 );
            graph.makeEdge( "a", "b", "cost", (float) 1 );
            graph.makeEdge( "b", "x", "cost", (double) 7 );
            graph.makeEdge( "b", "c", "cost", (long) 1 );
            graph.makeEdge( "c", "x", "cost", (int) 5 );
            graph.makeEdge( "c", "d", "cost", (byte) 1 );
            graph.makeEdge( "d", "x", "cost", (short) 3 );
            graph.makeEdge( "d", "e", "cost", (double) 1 );
            graph.makeEdge( "e", "x", "cost", (double) 1 );
            HashMap<Node,Double> seen1, seen2, dists1, dists2;
            seen1 = new HashMap<Node,Double>();
            seen2 = new HashMap<Node,Double>();
            dists1 = new HashMap<Node,Double>();
            dists2 = new HashMap<Node,Double>();
            DijstraIterator iter1 = new TestIterator( graph.getNode( "start" ),
                predecessors1, seen1, seen2, dists1, dists2, false );
            // while ( iter1.hasNext() && !limitReached() && !iter1.isDone() )
            assertTrue( iter1.next().equals( graph.getNode( "start" ) ) );
            assertTrue( iter1.next().equals( graph.getNode( "a" ) ) );
            assertTrue( seen1.get( graph.getNode( "x" ) ) == 10.0 );
            assertTrue( iter1.next().equals( graph.getNode( "b" ) ) );
            assertTrue( seen1.get( graph.getNode( "x" ) ) == 9.0 );
            assertTrue( iter1.next().equals( graph.getNode( "c" ) ) );
            assertTrue( seen1.get( graph.getNode( "x" ) ) == 8.0 );
            assertTrue( iter1.next().equals( graph.getNode( "d" ) ) );
            assertTrue( seen1.get( graph.getNode( "x" ) ) == 7.0 );
            assertTrue( iter1.next().equals( graph.getNode( "e" ) ) );
            assertTrue( seen1.get( graph.getNode( "x" ) ) == 6.0 );
            assertTrue( iter1.next().equals( graph.getNode( "x" ) ) );
            assertTrue( seen1.get( graph.getNode( "x" ) ) == 6.0 );
            assertFalse( iter1.hasNext() );
            int count = 0;
            // This code below is correct for the alternative priority queue
            // while ( iter1.hasNext() )
            // {
            // iter1.next();
            // ++count;
            // }
            // assertTrue( count == 4 );
            // assertTrue( seen1.get( graph.getNode( "x" ) ) == 6.0 );
            // Now test node limit
            seen1 = new HashMap<Node,Double>();
            seen2 = new HashMap<Node,Double>();
            dists1 = new HashMap<Node,Double>();
            dists2 = new HashMap<Node,Double>();
            iter1 = new TestIterator( graph.getNode( "start" ), predecessors1,
                seen1, seen2, dists1, dists2, false );
            this.numberOfNodesTraversed = 0;
            this.limitMaxNodesToTraverse( 3 );
            count = 0;
            while ( iter1.hasNext() )
            {
                iter1.next();
                ++count;
            }
            assertTrue( count == 3 );
        }
    }
}
