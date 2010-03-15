/*
 * Copyright 2008 Network Engine for Objects in Lund AB [neotechnology.com]
 * 
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphalgo.shortestPath;

import org.neo4j.graphalgo.shortestpath.FindSinglePath;
import org.neo4j.graphalgo.testUtil.Neo4jAlgoTestCase;
import org.neo4j.graphalgo.testUtil.SimpleGraphBuilder;

public class FindSinglePathTest extends Neo4jAlgoTestCase
{
    protected FindSinglePath getFindPath( SimpleGraphBuilder graph,
        String startNode, String endNode, int maxDepth )
    {
        return new FindSinglePath( graph.getNode( startNode ), graph
            .getNode( endNode ), MyRelTypes.R1, maxDepth );
    }

    /**
     * Test case for just a single node (path length zero)
     */
    public void testFindPathMinimal()
    {
        graph.makeNode( "lonely" );
        FindSinglePath findPath = getFindPath( graph, "lonely", "lonely",
            Integer.MAX_VALUE );
        assertTrue( findPath.getPathAsNodes().size() == 1 );
    }

    /**
     * Test case for a path of length zero, with some surrounding nodes
     */
    public void testFindPathMinimal2()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "a", "c", "cost", (double) 1 );
        graph.makeEdge( "a", "d", "cost", (double) 1 );
        graph.makeEdge( "a", "e", "cost", (double) 1 );
        graph.makeEdge( "b", "c", "cost", (double) 1 );
        graph.makeEdge( "c", "d", "cost", (double) 1 );
        graph.makeEdge( "d", "e", "cost", (double) 1 );
        graph.makeEdge( "e", "f", "cost", (double) 1 );
        FindSinglePath findPath = getFindPath( graph, "a", "a",
            Integer.MAX_VALUE );
        assertTrue( findPath.getPathAsNodes().size() == 1 );
        assertTrue( findPath.getPathAsRelationships().size() == 0 );
        assertTrue( findPath.getPath().size() == 1 );
    }

    public void testFindPathDepthLimit()
    {
        graph.makeEdgeChain( "a,b,c,d" );
        FindSinglePath findPath = getFindPath( graph, "a", "d", 3 );
        assertTrue( findPath.getPathAsNodes() != null );
        assertTrue( findPath.getPathAsNodes().size() == 4 );
        findPath = getFindPath( graph, "a", "d", 2 );
        assertTrue( findPath.getPathAsNodes() == null );
        findPath = getFindPath( graph, "a", "c", 2 );
        assertTrue( findPath.getPathAsNodes() != null );
        findPath = getFindPath( graph, "a", "c", 1 );
        assertTrue( findPath.getPathAsNodes() == null );
        findPath = getFindPath( graph, "a", "b", 1 );
        assertTrue( findPath.getPathAsNodes() != null );
        findPath = getFindPath( graph, "a", "b", 0 );
        assertTrue( findPath.getPathAsNodes() == null );
        findPath = getFindPath( graph, "d", "a", 3 );
        assertTrue( findPath.getPathAsNodes().size() == 4 );
        findPath = getFindPath( graph, "d", "b", 2 );
        assertTrue( findPath.getPathAsNodes().size() == 3 );
        assertTrue( findPath.getPathAsRelationships().size() == 2 );
        assertTrue( findPath.getPath().size() == 5 );
    }

    /**
     * /--2--A--7--B--2--\ S E \----7---C---7----/
     */
    public void testFindPathMoreComplex()
    {
        graph.makeEdge( "s", "c", "cost", (double) 7 );
        graph.makeEdge( "c", "e", "cost", (double) 7 );
        graph.makeEdge( "s", "a", "cost", (double) 2 );
        graph.makeEdge( "a", "b", "cost", (double) 7 );
        graph.makeEdge( "b", "e", "cost", (double) 2 );
        FindSinglePath findPath = getFindPath( graph, "s", "e",
            Integer.MAX_VALUE );
        assertTrue( findPath.getPathAsNodes() != null );
        assertTrue( findPath.getPathAsNodes().size() == 3 );
    }
}
