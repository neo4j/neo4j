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
package org.neo4j.graphalgo.path;

import common.Neo4jAlgoTestCase;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphalgo.BasicEvaluationContext;
import org.neo4j.graphalgo.EvaluationContext;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.path.ExactDepthPathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpander;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Transaction;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.GraphAlgoFactory.pathsWithLength;
import static org.neo4j.graphdb.PathExpanders.allTypesAndDirections;
import static org.neo4j.graphdb.PathExpanders.forDirection;

class TestExactDepthPathFinder extends Neo4jAlgoTestCase
{
    private static void createGraph()
    {
        graph.makeEdgeChain( "SOURCE,SUPER,c,d" );
        graph.makeEdgeChain( "SUPER,e,f" );
        graph.makeEdgeChain( "SUPER,5,6" );
        graph.makeEdgeChain( "SUPER,7,8" );
        graph.makeEdgeChain( "SUPER,r,SPIDER" );
        graph.makeEdgeChain( "SUPER,g,h,i,j,SPIDER" );
        graph.makeEdgeChain( "SUPER,k,l,m,SPIDER" );
        graph.makeEdgeChain( "SUPER,s,t,u,SPIDER" );
        graph.makeEdgeChain( "SUPER,v,w,x,y,SPIDER" );
        graph.makeEdgeChain( "SPIDER,n,o" );
        graph.makeEdgeChain( "SPIDER,p,q" );
        graph.makeEdgeChain( "SPIDER,1,2" );
        graph.makeEdgeChain( "SPIDER,3,4" );
        graph.makeEdgeChain( "SPIDER,TARGET" );
        graph.makeEdgeChain( "SOURCE,a,b,TARGET" );
        graph.makeEdgeChain( "SOURCE,z,9,0,TARGET" );
    }

    @Test
    void testSingle()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            final Set<String> possiblePaths = new HashSet<>();
            possiblePaths.add( "SOURCE,z,9,0,TARGET" );
            possiblePaths.add( "SOURCE,SUPER,r,SPIDER,TARGET" );
            createGraph();
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<Path> finder = newFinder( context );
            Path path = finder.findSinglePath( graph.getNode( "SOURCE" ), graph.getNode( "TARGET" ) );
            assertNotNull( path );
            assertThat( getPathDef( path ), is( in( possiblePaths ) ) );
            assertTrue( possiblePaths.contains( getPathDef( path ) ) );
            transaction.commit();
        }
    }

    @Test
    void testAll()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            createGraph();
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<Path> finder = newFinder( context );
            assertPaths( finder.findAllPaths( graph.getNode( "SOURCE" ), graph.getNode( "TARGET" ) ), "SOURCE,z,9,0,TARGET",
                    "SOURCE,SUPER,r,SPIDER,TARGET" );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleDirectionalGraph()
    {
        // ALL DIRECTED from (a) towards (g)
        //     (b) ----------------- (c)      length 3
        //   /                          \
        // (a) - (h) - (i) - (j) - (k) - (g)  length 5
        //   \                          /
        //     (d) - (e) ------------ (f)     length 4
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,b,c,g" );
            graph.makeEdgeChain( "a,d,e,f,g" );
            graph.makeEdgeChain( "a,h,i,j,k,g" );
            Node a = graph.getNode( "a" );
            Node g = graph.getNode( "g" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            assertPaths( new ExactDepthPathFinder( context, forDirection( Direction.OUTGOING ), 3, Integer.MAX_VALUE, false ).findAllPaths( a, g ), "a,b,c,g" );
            assertPaths( new ExactDepthPathFinder( context, forDirection( Direction.OUTGOING ), 4, Integer.MAX_VALUE, false ).findAllPaths( a, g ),
                    "a,d,e,f,g" );
            assertPaths( new ExactDepthPathFinder( context, forDirection( Direction.OUTGOING ), 5, Integer.MAX_VALUE, false ).findAllPaths( a, g ),
                    "a,h,i,j,k,g" );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleNondirectedGraph()
    {
        //     (b) ----------------- (c)      length 3
        //   /                          \
        // (a) - (h) - (i) - (j) - (k) - (g)  length 5
        //   \                          /
        //     (d) - (e) ------------ (f)     length 4
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,b,c,g" );
            graph.makeEdgeChain( "a,d,e,f,g" );
            graph.makeEdgeChain( "a,h,i,j,k,g" );
            Node a = graph.getNode( "a" );
            Node g = graph.getNode( "g" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 3, Integer.MAX_VALUE, false ).findAllPaths( a, g ), "a,b,c,g" );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 4, Integer.MAX_VALUE, false ).findAllPaths( a, g ), "a,d,e,f,g" );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 5, Integer.MAX_VALUE, false ).findAllPaths( a, g ), "a,h,i,j,k,g" );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleSimpleChainEvenDepth()
    {
        // (a) - (b) - (c)
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,b,c" );
            Node a = graph.getNode( "a" );
            Node c = graph.getNode( "c" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 2, Integer.MAX_VALUE, false ).findAllPaths( a, c ), "a,b,c" );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 2, Integer.MAX_VALUE, false ).findAllPaths( a, c ), "a,b,c" );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleSimpleChainOddDepth()
    {
        // (a) - (b) - (c) - (d)
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,b,c,d" );
            Node a = graph.getNode( "a" );
            Node d = graph.getNode( "d" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 3, Integer.MAX_VALUE, false ).findAllPaths( a, d ), "a,b,c,d" );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 3, Integer.MAX_VALUE, false ).findAllPaths( a, d ), "a,b,c,d" );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleNeighbouringNodes()
    {
        // (a) - (b)
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,b" );
            Node a = graph.getNode( "a" );
            Node b = graph.getNode( "b" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            ExactDepthPathFinder pathFinder = new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false );
            Iterable<Path> allPaths = pathFinder.findAllPaths( a, b );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths( a, b ), "a,b" );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths( a, b ), "a,b" );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleNeighbouringNodesWhenNotAlone()
    {
        // (a) - (b)
        //  |
        // (c)
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdge( "a", "b" );
            graph.makeEdge( "a", "c" );
            Node a = graph.getNode( "a" );
            Node b = graph.getNode( "b" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            ExactDepthPathFinder pathFinder = new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false );
            Iterable<Path> allPaths = pathFinder.findAllPaths( a, b );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths( a, b ), "a,b" );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths( a, b ), "a,b" );
            transaction.commit();
        }
    }

    @Test
    void shouldHandleNeighbouringNodesMultiplePaths()
    {
        // (a) = (b)
        //  |
        // (c)
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,b" );
            graph.makeEdgeChain( "a,b" );
            graph.makeEdgeChain( "a,c" );
            Node a = graph.getNode( "a" );
            Node b = graph.getNode( "b" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            ExactDepthPathFinder pathFinder = new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false );
            Iterable<Path> allPaths = pathFinder.findAllPaths( a, b );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths( a, b ), "a,b", "a,b" );
            assertPaths( new ExactDepthPathFinder( context, allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths( a, b ), "a,b", "a,b" );
            transaction.commit();
        }
    }

    @Test
    void testExactDepthFinder()
    {
        // Layout (a to k):
        //
        //     (a)--(c)--(g)--(k)
        //    /                /
        //  (b)-----(d)------(j)
        //   |        \      /
        //  (e)--(f)--(h)--(i)
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,c,g,k" );
            graph.makeEdgeChain( "a,b,d,j,k" );
            graph.makeEdgeChain( "b,e,f,h,i,j" );
            graph.makeEdgeChain( "d,h" );
            PathExpander<Object> expander = PathExpanders.forTypeAndDirection( MyRelTypes.R1, Direction.OUTGOING );
            Node a = graph.getNode( "a" );
            Node k = graph.getNode( "k" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            assertPaths( pathsWithLength( context, expander, 3 ).findAllPaths( a, k ), "a,c,g,k" );
            assertPaths( pathsWithLength( context, expander, 4 ).findAllPaths( a, k ), "a,b,d,j,k" );
            assertPaths( pathsWithLength( context, expander, 5 ).findAllPaths( a, k ) );
            assertPaths( pathsWithLength( context, expander, 6 ).findAllPaths( a, k ), "a,b,d,h,i,j,k" );
            assertPaths( pathsWithLength( context, expander, 7 ).findAllPaths( a, k ), "a,b,e,f,h,i,j,k" );
            assertPaths( pathsWithLength( context, expander, 8 ).findAllPaths( a, k ) );
            transaction.commit();
        }
    }

    @Test
    void testExactDepthPathsReturnsNoLoops()
    {
        // Layout:
        //
        // (a)-->(b)==>(c)-->(e)
        //        ^    /
        //         \  v
        //         (d)
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,b,c,d,b,c,e" );
            Node a = graph.getNode( "a" );
            Node e = graph.getNode( "e" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            assertPaths( pathsWithLength( context, PathExpanders.forType( MyRelTypes.R1 ), 3 ).findAllPaths( a, e ), "a,b,c,e", "a,b,c,e" );
            assertPaths( pathsWithLength( context, PathExpanders.forType( MyRelTypes.R1 ), 4 ).findAllPaths( a, e ), "a,b,d,c,e" );
            assertPaths( pathsWithLength( context, PathExpanders.forType( MyRelTypes.R1 ), 6 ).findAllPaths( a, e ) );
            transaction.commit();
        }
    }

    @Test
    void testExactDepthPathsLoopsAllowed()
    {
        // Layout:
        //
        // (a)-->(b)==>(c)-->(e)
        //        ^    /
        //         \  v
        //         (d)
        //
        try ( Transaction transaction = graphDb.beginTx() )
        {
            graph.makeEdgeChain( "a,b,c,d,b,c,e" );
            Node a = graph.getNode( "a" );
            Node e = graph.getNode( "e" );
            var context = new BasicEvaluationContext( transaction, graphDb );
            assertPaths( new ExactDepthPathFinder( context, forDirection( Direction.OUTGOING ), 6, Integer.MAX_VALUE, true ).findAllPaths( a, e ),
                    "a,b,c,d,b,c,e" );
            transaction.commit();
        }
    }

    private static PathFinder<Path> newFinder( EvaluationContext context )
    {
        return new ExactDepthPathFinder( context, allTypesAndDirections(), 4, 4, true );
    }
}
