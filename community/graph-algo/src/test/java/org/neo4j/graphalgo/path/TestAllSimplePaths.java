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

import common.Neo4jAlgoTestCase;
import org.junit.Test;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;

public class TestAllSimplePaths extends Neo4jAlgoTestCase
{
    protected PathFinder<Path> instantiatePathFinder( int maxDepth )
    {
        return GraphAlgoFactory.allSimplePaths( PathExpanders.allTypesAndDirections(), maxDepth );
    }

    @Test
    public void testCircularGraph()
    {
        /* Layout
         * 
         * (a)---(b)===(c)---(e)
         *         \   /
         *          (d)
         */
        graph.makeEdge( "a", "b" );
        graph.makeEdge( "b", "c" );
        graph.makeEdge( "b", "c" );
        graph.makeEdge( "b", "d" );
        graph.makeEdge( "c", "d" );
        graph.makeEdge( "c", "e" );

        PathFinder<Path> finder = instantiatePathFinder( 10 );
        Iterable<Path> paths = finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "e" ) );
        assertPaths( paths, "a,b,c,e", "a,b,c,e", "a,b,d,c,e" );
    }

    @Test
    public void testTripleRelationshipGraph()
    {
        /* Layout
         *          ___
         * (a)---(b)===(c)---(d)
         */
        graph.makeEdge( "a", "b" );
        graph.makeEdge( "b", "c" );
        graph.makeEdge( "b", "c" );
        graph.makeEdge( "b", "c" );
        graph.makeEdge( "c", "d" );
        
        PathFinder<Path> finder = instantiatePathFinder( 10 );
        Iterable<Path> paths = finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "d" ) );
        assertPaths( paths, "a,b,c,d", "a,b,c,d", "a,b,c,d" );
    }
}
