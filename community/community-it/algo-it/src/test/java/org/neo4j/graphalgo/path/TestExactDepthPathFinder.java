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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphalgo.GraphAlgoFactory.pathsWithLength;
import static org.neo4j.graphdb.PathExpanders.allTypesAndDirections;
import static org.neo4j.graphdb.PathExpanders.forDirection;

class TestExactDepthPathFinder extends Neo4jAlgoTestCase
{
    private static void createGraph( Transaction transaction )
    {
        graph.makeEdgeChain( transaction, "SOURCE,SUPER,c,d" );
        graph.makeEdgeChain( transaction, "SUPER,e,f" );
        graph.makeEdgeChain( transaction, "SUPER,5,6" );
        graph.makeEdgeChain( transaction, "SUPER,7,8" );
        graph.makeEdgeChain( transaction, "SUPER,r,SPIDER" );
        graph.makeEdgeChain( transaction, "SUPER,g,h,i,j,SPIDER" );
        graph.makeEdgeChain( transaction, "SUPER,k,l,m,SPIDER" );
        graph.makeEdgeChain( transaction, "SUPER,s,t,u,SPIDER" );
        graph.makeEdgeChain( transaction, "SUPER,v,w,x,y,SPIDER" );
        graph.makeEdgeChain( transaction, "SPIDER,n,o" );
        graph.makeEdgeChain( transaction, "SPIDER,p,q" );
        graph.makeEdgeChain( transaction, "SPIDER,1,2" );
        graph.makeEdgeChain( transaction, "SPIDER,3,4" );
        graph.makeEdgeChain( transaction, "SPIDER,TARGET" );
        graph.makeEdgeChain( transaction, "SOURCE,a,b,TARGET" );
        graph.makeEdgeChain( transaction, "SOURCE,z,9,0,TARGET" );
    }

    @Test
    void testSingle()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            final Set<String> possiblePaths = new HashSet<>();
            possiblePaths.add( "SOURCE,z,9,0,TARGET" );
            possiblePaths.add( "SOURCE,SUPER,r,SPIDER,TARGET" );
            createGraph( transaction );
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<Path> finder = newFinder( context );
            Path path = finder.findSinglePath( graph.getNode( transaction, "SOURCE" ), graph.getNode( transaction, "TARGET" ) );
            assertNotNull( path );
            assertThat( getPathDef( path ) ).isIn( possiblePaths );
            assertTrue( possiblePaths.contains( getPathDef( path ) ) );
            transaction.commit();
        }
    }

    @Test
    void testAll()
    {
        try ( Transaction transaction = graphDb.beginTx() )
        {
            createGraph( transaction );
            var context = new BasicEvaluationContext( transaction, graphDb );
            PathFinder<Path> finder = newFinder( context );
            assertPaths( finder.findAllPaths( graph.getNode( transaction, "SOURCE" ), graph.getNode( transaction, "TARGET" ) ), "SOURCE,z,9,0,TARGET",
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
            graph.makeEdgeChain( transaction, "a,b,c,g" );
            graph.makeEdgeChain( transaction, "a,d,e,f,g" );
            graph.makeEdgeChain( transaction, "a,h,i,j,k,g" );
            Node a = graph.getNode( transaction, "a" );
            Node g = graph.getNode( transaction, "g" );
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
            graph.makeEdgeChain( transaction, "a,b,c,g" );
            graph.makeEdgeChain( transaction, "a,d,e,f,g" );
            graph.makeEdgeChain( transaction, "a,h,i,j,k,g" );
            Node a = graph.getNode( transaction, "a" );
            Node g = graph.getNode( transaction, "g" );
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
            graph.makeEdgeChain( transaction, "a,b,c" );
            Node a = graph.getNode( transaction, "a" );
            Node c = graph.getNode( transaction, "c" );
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
            graph.makeEdgeChain( transaction, "a,b,c,d" );
            Node a = graph.getNode( transaction, "a" );
            Node d = graph.getNode( transaction, "d" );
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
            graph.makeEdgeChain( transaction, "a,b" );
            Node a = graph.getNode( transaction, "a" );
            Node b = graph.getNode( transaction, "b" );
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
            graph.makeEdge( transaction, "a", "b" );
            graph.makeEdge( transaction, "a", "c" );
            Node a = graph.getNode( transaction, "a" );
            Node b = graph.getNode( transaction, "b" );
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
            graph.makeEdgeChain( transaction, "a,b" );
            graph.makeEdgeChain( transaction, "a,b" );
            graph.makeEdgeChain( transaction, "a,c" );
            Node a = graph.getNode( transaction, "a" );
            Node b = graph.getNode( transaction, "b" );
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
            graph.makeEdgeChain( transaction, "a,c,g,k" );
            graph.makeEdgeChain( transaction, "a,b,d,j,k" );
            graph.makeEdgeChain( transaction, "b,e,f,h,i,j" );
            graph.makeEdgeChain( transaction, "d,h" );
            PathExpander<Object> expander = PathExpanders.forTypeAndDirection( MyRelTypes.R1, Direction.OUTGOING );
            Node a = graph.getNode( transaction, "a" );
            Node k = graph.getNode( transaction, "k" );
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
            graph.makeEdgeChain( transaction, "a,b,c,d,b,c,e" );
            Node a = graph.getNode( transaction, "a" );
            Node e = graph.getNode( transaction, "e" );
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
            graph.makeEdgeChain( transaction, "a,b,c,d,b,c,e" );
            Node a = graph.getNode( transaction, "a" );
            Node e = graph.getNode( transaction, "e" );
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
