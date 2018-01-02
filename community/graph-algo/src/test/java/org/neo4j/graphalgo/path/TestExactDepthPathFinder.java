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
package org.neo4j.graphalgo.path;

import org.junit.Test;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.path.ExactDepthPathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.kernel.Traversal;

import common.Neo4jAlgoTestCase;
import static org.junit.Assert.assertNotNull;

public class TestExactDepthPathFinder extends Neo4jAlgoTestCase
{
    public void createGraph()
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

    private PathFinder<Path> newFinder()
    {
        return new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 4, 4, true );
    }

    @Test
    public void testSingle()
    {
        createGraph();
        PathFinder<Path> finder = newFinder();
        Path path = finder.findSinglePath( graph.getNode( "SOURCE" ), graph.getNode( "TARGET" ) );
        assertNotNull( path );
        assertPathDef( path, "SOURCE", "z", "9", "0", "TARGET" );
    }

    @Test
    public void testAll()
    {
        createGraph();
        assertPaths( newFinder().findAllPaths( graph.getNode( "SOURCE" ), graph.getNode( "TARGET" ) ),
                "SOURCE,z,9,0,TARGET", "SOURCE,SUPER,r,SPIDER,TARGET" );
    }

    @Test
    public void shouldHandleDirectionalGraph()
    {
        // ALL DIRECTED from (a) towards (g)
        //     (b) ----------------- (c)      length 3
        //   /                          \
        // (a) - (h) - (i) - (j) - (k) - (g)  length 5
        //   \                          /
        //     (d) - (e) ------------ (f)     length 4
        graph.makeEdgeChain( "a,b,c,g" );
        graph.makeEdgeChain( "a,d,e,f,g" );
        graph.makeEdgeChain( "a,h,i,j,k,g" );
        Node a = graph.getNode( "a" );
        Node g = graph.getNode( "g" );
        assertPaths( new ExactDepthPathFinder( PathExpanders.forDirection( Direction.OUTGOING ), 3, Integer.MAX_VALUE,
                false ).findAllPaths( a, g ), "a,b,c,g" );
        assertPaths( new ExactDepthPathFinder( PathExpanders.forDirection( Direction.OUTGOING ), 4, Integer.MAX_VALUE,
                false ).findAllPaths( a, g ), "a,d,e,f,g" );
        assertPaths( new ExactDepthPathFinder( PathExpanders.forDirection( Direction.OUTGOING ), 5, Integer.MAX_VALUE,
                false ).findAllPaths( a, g ), "a,h,i,j,k,g" );
    }

    @Test
    public void shouldHandleNondirectedGraph()
    {
        //     (b) ----------------- (c)      length 3
        //   /                          \
        // (a) - (h) - (i) - (j) - (k) - (g)  length 5
        //   \                          /
        //     (d) - (e) ------------ (f)     length 4
        graph.makeEdgeChain( "a,b,c,g" );
        graph.makeEdgeChain( "a,d,e,f,g" );
        graph.makeEdgeChain( "a,h,i,j,k,g" );
        Node a = graph.getNode( "a" );
        Node g = graph.getNode( "g" );
        assertPaths(
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 3, Integer.MAX_VALUE, false ).findAllPaths(
                        a, g ), "a,b,c,g" );
        assertPaths(
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 4, Integer.MAX_VALUE, false ).findAllPaths(
                        a, g ), "a,d,e,f,g" );
        assertPaths(
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 5, Integer.MAX_VALUE, false ).findAllPaths(
                        a, g ), "a,h,i,j,k,g" );
    }

    @Test
    public void shouldHandleSimpleChainEvenDepth()
    {
        // (a) - (b) - (c)
        graph.makeEdgeChain( "a,b,c" );
        Node a = graph.getNode( "a" );
        Node c = graph.getNode( "c" );
        assertPaths(
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 2, Integer.MAX_VALUE, false ).findAllPaths(
                        a, c ), "a,b,c" );
        assertPaths(
                new ExactDepthPathFinder( Traversal.expanderForAllTypes(), 2, Integer.MAX_VALUE, false ).findAllPaths(
                        a, c ), "a,b,c" );
    }

    @Test
    public void shouldHandleSimpleChainOddDepth()
    {
        // (a) - (b) - (c) - (d)
        graph.makeEdgeChain( "a,b,c,d" );
        Node a = graph.getNode( "a" );
        Node d = graph.getNode( "d" );
        assertPaths(
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 3, Integer.MAX_VALUE, false ).findAllPaths(
                        a, d ), "a,b,c,d" );
        assertPaths(
                new ExactDepthPathFinder( Traversal.expanderForAllTypes(), 3, Integer.MAX_VALUE, false ).findAllPaths(
                        a, d ), "a,b,c,d" );
    }

    @Test
    public void shouldHandleNeighbouringNodes()
    {
        // (a) - (b)
        graph.makeEdgeChain( "a,b" );
        Node a = graph.getNode( "a" );
        Node b = graph.getNode( "b" );
        ExactDepthPathFinder pathFinder =
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 1, Integer.MAX_VALUE, false );
        Iterable<Path> allPaths = pathFinder.findAllPaths( a, b );
        assertPaths(
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths(
                        a, b ), "a,b" );
        assertPaths(
                new ExactDepthPathFinder( Traversal.expanderForAllTypes(), 1, Integer.MAX_VALUE, false ).findAllPaths(
                        a, b ), "a,b" );
    }

    @Test
    public void shouldHandleNeighbouringNodesWhenNotAlone()
    {
        // (a) - (b)
        //  |
        // (c)
        graph.makeEdge( "a", "b" );
        graph.makeEdge( "a", "c" );
        Node a = graph.getNode( "a" );
        Node b = graph.getNode( "b" );
        ExactDepthPathFinder pathFinder =
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 1, Integer.MAX_VALUE, false );
        Iterable<Path> allPaths = pathFinder.findAllPaths( a, b );
        assertPaths(
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths(
                        a, b ), "a,b" );
        assertPaths(
                new ExactDepthPathFinder( Traversal.expanderForAllTypes(), 1, Integer.MAX_VALUE, false ).findAllPaths(
                        a, b ), "a,b" );
    }
    
    @Test
    public void shouldHandleNeighbouringNodesMultiplePaths()
    {
        // (a) = (b)
        //  |
        // (c)
        graph.makeEdgeChain( "a,b" );
        graph.makeEdgeChain( "a,b" );
        graph.makeEdgeChain( "a,c" );
        Node a = graph.getNode( "a" );
        Node b = graph.getNode( "b" );
        ExactDepthPathFinder pathFinder =
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 1, Integer.MAX_VALUE, false );
        Iterable<Path> allPaths = pathFinder.findAllPaths( a, b );
        assertPaths(
                new ExactDepthPathFinder( PathExpanders.allTypesAndDirections(), 1, Integer.MAX_VALUE, false ).findAllPaths(
                        a, b ), "a,b", "a,b" );
        assertPaths(
                new ExactDepthPathFinder( Traversal.expanderForAllTypes(), 1, Integer.MAX_VALUE, false ).findAllPaths(
                        a, b ), "a,b", "a,b" );
    }

    @Test
    public void testExactDepthFinder()
    {
        // Layout (a to k):
        //
        //     (a)--(c)--(g)--(k)
        //    /                /
        //  (b)-----(d)------(j)
        //   |        \      /
        //  (e)--(f)--(h)--(i)
        // 
        graph.makeEdgeChain( "a,c,g,k" );
        graph.makeEdgeChain( "a,b,d,j,k" );
        graph.makeEdgeChain( "b,e,f,h,i,j" );
        graph.makeEdgeChain( "d,h" );
        RelationshipExpander expander = Traversal.expanderForTypes( MyRelTypes.R1, Direction.OUTGOING );
        Node a = graph.getNode( "a" );
        Node k = graph.getNode( "k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 3 ).findAllPaths( a, k ), "a,c,g,k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 4 ).findAllPaths( a, k ), "a,b,d,j,k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 5 ).findAllPaths( a, k ) );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 6 ).findAllPaths( a, k ), "a,b,d,h,i,j,k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 7 ).findAllPaths( a, k ), "a,b,e,f,h,i,j,k" );
        assertPaths( GraphAlgoFactory.pathsWithLength( expander, 8 ).findAllPaths( a, k ) );
    }

    @Test
    public void testExactDepthPathsReturnsNoLoops()
    {
        // Layout:
        //
        // (a)-->(b)==>(c)-->(e)
        //        ^    /
        //         \  v
        //         (d)
        //
        graph.makeEdgeChain( "a,b,c,d,b,c,e" );
        Node a = graph.getNode( "a" );
        Node e = graph.getNode( "e" );
        assertPaths(
                GraphAlgoFactory.pathsWithLength( Traversal.expanderForTypes( MyRelTypes.R1 ), 3 ).findAllPaths( a, e ),
                "a,b,c,e", "a,b,c,e" );
        assertPaths(
                GraphAlgoFactory.pathsWithLength( Traversal.expanderForTypes( MyRelTypes.R1 ), 4 ).findAllPaths( a, e ),
                "a,b,d,c,e" );
        assertPaths( GraphAlgoFactory.pathsWithLength( Traversal.expanderForTypes( MyRelTypes.R1 ), 6 ).findAllPaths(
                a, e ) );
    }

    @Test
    public void testExactDepthPathsLoopsAllowed()
    {
        // Layout:
        //
        // (a)-->(b)==>(c)-->(e)
        //        ^    /
        //         \  v
        //         (d)
        //
        graph.makeEdgeChain( "a,b,c,d,b,c,e" );
        Node a = graph.getNode( "a" );
        Node e = graph.getNode( "e" );
        assertPaths( new ExactDepthPathFinder( PathExpanders.forDirection( Direction.OUTGOING ), 6, Integer.MAX_VALUE,
                true ).findAllPaths( a, e ), "a,b,c,d,b,c,e" );
    }
}
