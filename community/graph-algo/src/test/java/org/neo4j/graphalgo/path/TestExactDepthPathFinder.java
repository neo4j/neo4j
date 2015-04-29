/*
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
package org.neo4j.graphalgo.path;

import org.junit.Before;
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
import common.Neo4jAlgoTestCase.MyRelTypes;
import static org.junit.Assert.assertNotNull;

public class TestExactDepthPathFinder extends Neo4jAlgoTestCase
{
    public void createGraph()
    {
        graph.makeEdgeChain( "SOURCE,a,b,TARGET" );
        graph.makeEdgeChain( "SOURCE,SUPER,c,d" );
        graph.makeEdgeChain( "SUPER,e,f" );
        graph.makeEdgeChain( "SUPER,g,h,i,j,SPIDER" );
        graph.makeEdgeChain( "SUPER,k,l,m,SPIDER" );
        graph.makeEdgeChain( "SUPER,r,SPIDER" );
        graph.makeEdgeChain( "SPIDER,n,o" );
        graph.makeEdgeChain( "SPIDER,p,q" );
        graph.makeEdgeChain( "SPIDER,TARGET" );
        graph.makeEdgeChain( "SUPER,s,t,u,SPIDER" );
        graph.makeEdgeChain( "SUPER,v,w,x,y,SPIDER" );
        graph.makeEdgeChain( "SPIDER,1,2" );
        graph.makeEdgeChain( "SPIDER,3,4" );
        graph.makeEdgeChain( "SUPER,5,6" );
        graph.makeEdgeChain( "SUPER,7,8" );
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
}
