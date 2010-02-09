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
package org.neo4j.graphalgo;

import java.util.List;

import org.neo4j.graphalgo.testUtil.Neo4jAlgoTestCase;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

public class AllSimplePathsTest extends Neo4jAlgoTestCase
{
    public AllSimplePathsTest( String name )
    {
        super( name );
    }

    public void testRun()
    {
        // Make a graph of four different paths
        graph.makeEdgeChain( "a,b1,c,d1,e" );
        graph.makeEdgeChain( "a,b2,c,d2,e" );
        // First we try insufficient depth
        AllSimplePaths allSimplePaths = new AllSimplePaths(
            graph.getNode( "a" ), graph.getNode( "e" ), 3, Direction.OUTGOING,
            MyRelTypes.R1 );
        assertTrue( allSimplePaths.getPaths().size() == 0 );
        // Then exactly enough
        allSimplePaths = new AllSimplePaths( graph.getNode( "a" ), graph
            .getNode( "e" ), 4, Direction.OUTGOING, MyRelTypes.R1 );
        assertTrue( allSimplePaths.getPaths().size() == 4 );
        // Then a lot
        allSimplePaths = new AllSimplePaths( graph.getNode( "a" ), graph
            .getNode( "e" ), 100, Direction.OUTGOING, MyRelTypes.R1 );
        List<List<?>> paths = allSimplePaths.getPaths();
        List<List<Node>> pathsAsNodes = allSimplePaths.getPathsAsNodes();
        assertTrue( paths.size() == 4 );
        assertTrue( pathsAsNodes.size() == 4 );
        // Then we test the resulting paths
        boolean b1 = false, b2 = false, d1 = false, d2 = false;
        for ( List<Node> nodePath : pathsAsNodes )
        {
            assertTrue( nodePath.size() == 5 );
            assertTrue( nodePath.get( 0 ).equals( graph.getNode( "a" ) ) );
            assertTrue( nodePath.get( 2 ).equals( graph.getNode( "c" ) ) );
            assertTrue( nodePath.get( 4 ).equals( graph.getNode( "e" ) ) );
            if ( nodePath.get( 1 ).equals( graph.getNode( "b1" ) ) )
            {
                b1 = true;
            }
            if ( nodePath.get( 1 ).equals( graph.getNode( "b2" ) ) )
            {
                b2 = true;
            }
            if ( nodePath.get( 3 ).equals( graph.getNode( "d1" ) ) )
            {
                d1 = true;
            }
            if ( nodePath.get( 3 ).equals( graph.getNode( "d2" ) ) )
            {
                d2 = true;
            }
        }
        assertTrue( b1 );
        assertTrue( b2 );
        assertTrue( d1 );
        assertTrue( d2 );
    }

    public void testSinglePath()
    {
        // Make a graph of four different paths
        graph.makeEdgeChain( "a,b,c,d,e" );
        // First we try insufficient depth
        AllSimplePaths allSimplePaths = new AllSimplePaths(
            graph.getNode( "a" ), graph.getNode( "e" ), 3, Direction.OUTGOING,
            MyRelTypes.R1 );
        assertTrue( allSimplePaths.getPaths().size() == 0 );
        // Then exactly enough
        allSimplePaths = new AllSimplePaths( graph.getNode( "a" ), graph
            .getNode( "e" ), 4, Direction.OUTGOING, MyRelTypes.R1 );
        assertTrue( allSimplePaths.getPaths().size() == 1 );
        for ( List<Node> nodePath : allSimplePaths.getPathsAsNodes() )
        {
            assertTrue( nodePath.size() == 5 );
            assertTrue( nodePath.get( 0 ).equals( graph.getNode( "a" ) ) );
            assertTrue( nodePath.get( 1 ).equals( graph.getNode( "b" ) ) );
            assertTrue( nodePath.get( 2 ).equals( graph.getNode( "c" ) ) );
            assertTrue( nodePath.get( 3 ).equals( graph.getNode( "d" ) ) );
            assertTrue( nodePath.get( 4 ).equals( graph.getNode( "e" ) ) );
        }
        // Then exactly enough for the traversals to reach the other sides
        allSimplePaths = new AllSimplePaths( graph.getNode( "a" ), graph
            .getNode( "e" ), 8, Direction.OUTGOING, MyRelTypes.R1 );
        assertTrue( allSimplePaths.getPaths().size() == 1 );
        for ( List<Node> nodePath : allSimplePaths.getPathsAsNodes() )
        {
            assertTrue( nodePath.size() == 5 );
            assertTrue( nodePath.get( 0 ).equals( graph.getNode( "a" ) ) );
            assertTrue( nodePath.get( 1 ).equals( graph.getNode( "b" ) ) );
            assertTrue( nodePath.get( 2 ).equals( graph.getNode( "c" ) ) );
            assertTrue( nodePath.get( 3 ).equals( graph.getNode( "d" ) ) );
            assertTrue( nodePath.get( 4 ).equals( graph.getNode( "e" ) ) );
        }
        // Then a lot
        allSimplePaths = new AllSimplePaths( graph.getNode( "a" ), graph
            .getNode( "e" ), 100, Direction.OUTGOING, MyRelTypes.R1 );
        List<List<?>> paths = allSimplePaths.getPaths();
        List<List<Node>> pathsAsNodes = allSimplePaths.getPathsAsNodes();
        assertTrue( paths.size() == 1 );
        assertTrue( pathsAsNodes.size() == 1 );
        for ( List<Node> nodePath : allSimplePaths.getPathsAsNodes() )
        {
            assertTrue( nodePath.size() == 5 );
            assertTrue( nodePath.get( 0 ).equals( graph.getNode( "a" ) ) );
            assertTrue( nodePath.get( 1 ).equals( graph.getNode( "b" ) ) );
            assertTrue( nodePath.get( 2 ).equals( graph.getNode( "c" ) ) );
            assertTrue( nodePath.get( 3 ).equals( graph.getNode( "d" ) ) );
            assertTrue( nodePath.get( 4 ).equals( graph.getNode( "e" ) ) );
        }
    }
}
