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

import java.util.List;

import org.neo4j.graphalgo.shortestpath.FindPath;
import org.neo4j.graphalgo.testUtil.Neo4jAlgoTestCase;
import org.neo4j.graphalgo.testUtil.SimpleGraphBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;

public class FindPathTest extends Neo4jAlgoTestCase
{
    public FindPathTest( String arg0 )
    {
        super( arg0 );
    }

    protected FindPath getFindPath( SimpleGraphBuilder graph, String startNode,
            String endNode )
    {
        return new FindPath( graph.getNode( startNode ),
                graph.getNode( endNode ), Direction.BOTH, MyRelTypes.R1 );
    }

    /**
     * Test case for just a single node (path length zero)
     */
    public void testFindPathMinimal()
    {
        graph.makeNode( "lonely" );
        FindPath findPath = getFindPath( graph, "lonely", "lonely" );
        assertTrue( findPath.getCost() == 0 );
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
        FindPath findPath = getFindPath( graph, "a", "a" );
        assertTrue( findPath.getCost() == 0 );
        assertTrue( findPath.getPathAsNodes().size() == 1 );
        assertTrue( findPath.getPathAsRelationships().size() == 0 );
        assertTrue( findPath.getPath().size() == 1 );
        assertTrue( findPath.getPathsAsNodes().size() == 1 );
    }

    /**
     * Limiting the cost should not get any path
     */
    public void testMaxCost()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "a", "c", "cost", (double) 1 );
        graph.makeEdge( "c", "d", "cost", (double) 1 );
        graph.makeEdge( "b", "d", "cost", (double) 1 );
        graph.makeEdge( "d", "e", "cost", (double) 1 );
        graph.makeEdge( "e", "f", "cost", (double) 1 );
        FindPath findPath = new FindPath( graph.getNode( "a" ),
                graph.getNode( "d" ), 0, Direction.OUTGOING, MyRelTypes.R1 );
        List<List<PropertyContainer>> paths = findPath.getPaths();
        assertTrue( paths.isEmpty() );
        assertNull( findPath.getCost() );
        findPath = new FindPath( graph.getNode( "a" ), graph.getNode( "d" ), 2,
                Direction.OUTGOING, MyRelTypes.R1 );
        assertTrue( findPath.getCost() == 2 );
        assertTrue( findPath.getPathAsNodes().size() == 3 );
        assertTrue( findPath.getPathAsRelationships().size() == 2 );

        findPath = new FindPath( graph.getNode( "a" ), graph.getNode( "e" ), 2,
                Direction.OUTGOING, MyRelTypes.R1 );
        assertNull( findPath.getCost() );
        assertTrue( findPath.getPaths().isEmpty() );

        findPath = new FindPath( graph.getNode( "a" ), graph.getNode( "e" ), 3,
                Direction.OUTGOING, MyRelTypes.R1 );
        assertTrue( findPath.getCost() == 3 );
        assertTrue( findPath.getPathAsNodes().size() == 4 );
        assertTrue( findPath.getPathAsRelationships().size() == 3 );
    }

    public void testFindPathChain()
    {
        graph.makeEdge( "a", "b", "cost", (double) 1 );
        graph.makeEdge( "b", "c", "cost", (double) 2 );
        graph.makeEdge( "c", "d", "cost", (double) 3 );
        FindPath findPath = getFindPath( graph, "a", "d" );
        assertTrue( findPath.getCost() == 3 );
        assertTrue( findPath.getPathAsNodes() != null );
        assertTrue( findPath.getPathAsNodes().size() == 4 );
        assertTrue( findPath.getPathsAsNodes().size() == 1 );
        findPath = getFindPath( graph, "d", "a" );
        assertTrue( findPath.getCost() == 3 );
        assertTrue( findPath.getPathAsNodes().size() == 4 );
        findPath = getFindPath( graph, "d", "b" );
        assertTrue( findPath.getCost() == 2 );
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
        FindPath findPath = getFindPath( graph, "s", "e" );
        assertTrue( findPath.getCost() == 2 );
        assertTrue( findPath.getPathAsNodes() != null );
        assertTrue( findPath.getPathAsNodes().size() == 3 );
        assertTrue( findPath.getPathsAsNodes().size() == 1 );
    }

    /**
     * A triangle should generate one path between every pair of nodes.
     */
    public void testTriangle()
    {
        graph.makeEdge( "a", "b", "cost", (double) 0 );
        graph.makeEdge( "b", "c", "cost", (double) 0 );
        graph.makeEdge( "c", "a", "cost", (double) 0 );
        String[] nodes = { "a", "b", "c" };
        for ( String node1 : nodes )
        {
            for ( String node2 : nodes )
            {
                FindPath findPath = getFindPath( graph, node1, node2 );
                int nrPaths = findPath.getPathsAsNodes().size();
                if ( !node1.equals( node2 ) )
                {
                    assertTrue( "Number of paths (" + node1 + "->" + node2
                                + "): " + nrPaths, nrPaths == 1 );
                }
            }
        }
    }

    /**
     * From each direction 2 ways are possible so 4 ways should be the total.
     */
    public void test1()
    {
        graph.makeEdge( "a", "b" );
        graph.makeEdge( "b", "d" );
        graph.makeEdge( "a", "c" );
        graph.makeEdge( "c", "d" );
        graph.makeEdge( "d", "e" );
        graph.makeEdge( "e", "f" );
        graph.makeEdge( "f", "h" );
        graph.makeEdge( "e", "g" );
        graph.makeEdge( "g", "h" );
        FindPath findPath = getFindPath( graph, "a", "h" );
        assertTrue( findPath.getPaths().size() == 4 );
        assertTrue( findPath.getPathsAsNodes().size() == 4 );
        assertTrue( findPath.getPathsAsRelationships().size() == 4 );
        assertTrue( findPath.getCost() == 5.0 );
    }

    public void testMultiplePaths()
    {
        Relationship edgeAB = graph.makeEdge( "a", "b" );
        Relationship edgeBC = graph.makeEdge( "b", "c" );
        Relationship edgeCD = graph.makeEdge( "c", "d" );
        Relationship edgeDE = graph.makeEdge( "d", "e" );
        Relationship edgeAB2 = graph.makeEdge( "a", "b2" );
        Relationship edgeB2C = graph.makeEdge( "b2", "c" );
        Relationship edgeCD2 = graph.makeEdge( "c", "d2" );
        Relationship edgeD2E = graph.makeEdge( "d2", "e" );
        FindPath findPath = getFindPath( graph, "a", "e" );
        // path discovery flags
        boolean pathBD = false;
        boolean pathB2D = false;
        boolean pathBD2 = false;
        boolean pathB2D2 = false;
        List<List<PropertyContainer>> paths = findPath.getPaths();
        assertTrue( paths.size() == 4 );
        for ( List<PropertyContainer> path : paths )
        {
            assertTrue( path.size() == 9 );
            assertTrue( path.get( 0 ).equals( graph.getNode( "a" ) ) );
            assertTrue( path.get( 4 ).equals( graph.getNode( "c" ) ) );
            assertTrue( path.get( 8 ).equals( graph.getNode( "e" ) ) );
            // first choice
            if ( path.get( 2 ).equals( graph.getNode( "b" ) ) )
            {
                assertTrue( path.get( 1 ).equals( edgeAB ) );
                assertTrue( path.get( 3 ).equals( edgeBC ) );
            }
            else
            {
                assertTrue( path.get( 1 ).equals( edgeAB2 ) );
                assertTrue( path.get( 2 ).equals( graph.getNode( "b2" ) ) );
                assertTrue( path.get( 3 ).equals( edgeB2C ) );
            }
            // second choice
            if ( path.get( 6 ).equals( graph.getNode( "d" ) ) )
            {
                assertTrue( path.get( 5 ).equals( edgeCD ) );
                assertTrue( path.get( 7 ).equals( edgeDE ) );
            }
            else
            {
                assertTrue( path.get( 5 ).equals( edgeCD2 ) );
                assertTrue( path.get( 6 ).equals( graph.getNode( "d2" ) ) );
                assertTrue( path.get( 7 ).equals( edgeD2E ) );
            }
            // combinations
            if ( path.get( 2 ).equals( graph.getNode( "b" ) ) )
            {
                if ( path.get( 6 ).equals( graph.getNode( "d" ) ) )
                {
                    pathBD = true;
                }
                else if ( path.get( 6 ).equals( graph.getNode( "d2" ) ) )
                {
                    pathBD2 = true;
                }
            }
            else
            {
                if ( path.get( 6 ).equals( graph.getNode( "d" ) ) )
                {
                    pathB2D = true;
                }
                else if ( path.get( 6 ).equals( graph.getNode( "d2" ) ) )
                {
                    pathB2D2 = true;
                }
            }
        }
        assertTrue( pathBD );
        assertTrue( pathB2D );
        assertTrue( pathBD2 );
        assertTrue( pathB2D2 );
    }
}
