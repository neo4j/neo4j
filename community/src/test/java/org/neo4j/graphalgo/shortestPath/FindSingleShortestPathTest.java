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

import org.neo4j.graphalgo.shortestpath.FindSingleShortestPath;
import org.neo4j.graphalgo.testUtil.Neo4jAlgoTestCase;
import org.neo4j.graphalgo.testUtil.SimpleGraphBuilder;
import org.neo4j.graphdb.Node;

public class FindSingleShortestPathTest extends Neo4jAlgoTestCase
{
    protected FindSingleShortestPath getFindPath( SimpleGraphBuilder graph,
        String startNode, String endNode, int maxDepth )
    {
        return new FindSingleShortestPath( graph.getNode( startNode ), graph
            .getNode( endNode ), MyRelTypes.R1, maxDepth );
    }

    /**
     * Test case for just a single node (path length zero)
     */
    public void testFindPathMinimal()
    {
        graph.makeNode( "lonely" );
        FindSingleShortestPath findPath = getFindPath( graph, "lonely", "lonely",
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
        FindSingleShortestPath findPath = getFindPath( graph, "a", "a",
            Integer.MAX_VALUE );
        assertTrue( findPath.getPathAsNodes().size() == 1 );
        assertTrue( findPath.getPathAsRelationships().size() == 0 );
        assertTrue( findPath.getPath().size() == 1 );
    }

    public void testFindPathDepthLimit()
    {
        graph.makeEdgeChain( "a,b,c,d" );
        FindSingleShortestPath findPath = getFindPath( graph, "a", "d", 3 );
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
        FindSingleShortestPath findPath = getFindPath( graph, "s", "e",
            Integer.MAX_VALUE );
        assertTrue( findPath.getPathAsNodes() != null );
        assertTrue( findPath.getPathAsNodes().size() == 3 );
    }
    /**
     * /--2--A--7--B--2--\ S E \----7---C---7----/
     */
    public void testPathConstruct()
    {
        graph.makeEdge( "s", "a", "cost", (double) 1 );
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "a", "c", "cost", (double) 1 );
        graph.makeEdge( "b", "d", "cost", (double) 1 );
        graph.makeEdge( "d", "e", "cost", (double) 1 );
        graph.makeEdge( "c", "e", "cost", (double) 1 );
        graph.makeEdge( "b", "f", "cost", (double) 1 );
        graph.makeEdge( "d", "f", "cost", (double) 1 );
        graph.makeEdge( "e", "f", "cost", (double) 1 );
        FindSingleShortestPath findPath = getFindPath( graph, "s", "f",
            Integer.MAX_VALUE );
        assertPath( findPath, graph, "s", "a", "b", "f" ); 
    }
    
    public void testSwitchDepths()
    {
        graph.makeEdge( "big", "a" );
        graph.makeEdge( "big", "b" );
        graph.makeEdge( "big", "c" );
        graph.makeEdge( "big", "d" );
        graph.makeEdge( "big", "e" );
        graph.makeEdge( "big", "f" );
        graph.makeEdge( "a", "g" );
        graph.makeEdge( "a", "h" );
        graph.makeEdge( "b", "i" );
        graph.makeEdge( "b", "j" );
        graph.makeEdge( "c", "k" );
        graph.makeEdge( "d", "l" );
        graph.makeEdge( "g", "m" );
        graph.makeEdge( "g", "n" );
        graph.makeEdge( "n", "o" );
        graph.makeEdge( "o", "small" );
        graph.makeEdge( "small", "p" );
 
        // This should make the finder switch depths so that the one with
        // least relationships ("big" or "small"; "small" in this case)
        // will go deepest. But how do we make sure?
        FindSingleShortestPath finder = getFindPath( graph, "small", "big", 5 );
        assertPath( finder, graph, "small", "o", "n", "g", "a", "big" );
    }

    private void assertPath( FindSingleShortestPath finder,
            SimpleGraphBuilder graph, String... path )
    {
        assertEquals( path.length, finder.getPathAsNodes().size() );
        int counter = 0;
        for ( Node pathNode : finder.getPathAsNodes() )
        {
            Node graphNode = graph.getNode( path[ counter++ ] );
            assertEquals( graphNode.getProperty( SimpleGraphBuilder.KEY_ID ) +
                    ", " + pathNode.getProperty( SimpleGraphBuilder.KEY_ID ),
                    graphNode, pathNode );
        }
    }
}
