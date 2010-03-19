package org.neo4j.graphalgo.shortestPath;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.junit.Test;
import org.neo4j.graphalgo.Path;
import org.neo4j.graphalgo.RelationshipExpander;
import org.neo4j.graphalgo.shortestpath.LevelShortestPathsFinder;
import org.neo4j.graphalgo.shortestpath.PathFinder;
import org.neo4j.graphalgo.shortestpath.SingleStepShortestPathsFinder;
import org.neo4j.graphalgo.testUtil.Neo4jAlgoTestCase;
import org.neo4j.graphalgo.testUtil.SimpleGraphBuilder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;

public class TestSingleStepShortestPath extends Neo4jAlgoTestCase
{
    protected PathFinder instantiatePathFinder( int maxDepth )
    {
        return new SingleStepShortestPathsFinder( graphDb, maxDepth,
                RelationshipExpander.forTypes( MyRelTypes.R1, Direction.BOTH ) );
    }
    
    @Test
    public void testSimplestGraph()
    {
        // Layout:
        //    __
        //   /  \
        // (s)  (t)
        //   \__/
        graph.makeEdge( "s", "t" );
        graph.makeEdge( "s", "t" );

        PathFinder finder = instantiatePathFinder( 1 );
        Collection<Path> paths = finder.findPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
        assertEquals( 2, paths.size() );
        assertPaths( paths, "s,t", "s,t" );
    }
    
    @Test
    public void testAnotherSimpleGraph()
    {
        // Layout:
        //   (m)
        //   /  \
        // (s)  (o)---(t)
        //   \  /       \
        //   (n)---(p)---(q)
        graph.makeEdge( "s", "m" );
        graph.makeEdge( "m", "o" );
        graph.makeEdge( "s", "n" );
        graph.makeEdge( "n", "p" );
        graph.makeEdge( "p", "q" );
        graph.makeEdge( "q", "t" );
        graph.makeEdge( "n", "o" );
        graph.makeEdge( "o", "t" );

        PathFinder finder = instantiatePathFinder( 6 );
        Collection<Path> paths = finder.findPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
        assertPaths( paths, "s,m,o,t", "s,n,o,t" );
    }
    
    @Test
    public void testCrossedCircle()
    {
        // Layout:
        //    (s)
        //   /   \
        // (3)   (1)
        //  | \ / |
        //  | / \ |
        // (4)   (5)
        //   \   /
        //    (t)
        graph.makeEdge( "s", "1" );
        graph.makeEdge( "s", "3" );
        graph.makeEdge( "1", "2" );
        graph.makeEdge( "1", "4" );
        graph.makeEdge( "3", "2" );
        graph.makeEdge( "3", "4" );
        graph.makeEdge( "2", "t" );
        graph.makeEdge( "4", "t" );
        
        PathFinder singleStepFinder = instantiatePathFinder( 3 );
        Collection<Path> paths = singleStepFinder.findPaths( graph.getNode( "s" ),
                graph.getNode( "t" ) );
        assertPaths( paths, "s,1,2,t", "s,1,4,t", "s,3,2,t", "s,3,4,t" );

        LevelShortestPathsFinder levelFinder = new LevelShortestPathsFinder( 3, MyRelTypes.R1,
                Direction.BOTH );
        paths = levelFinder.findPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
        assertPaths( paths, "s,1,2,t", "s,1,4,t", "s,3,2,t", "s,3,4,t" );
    }

    private void assertPaths( Collection<Path> paths, String... pathDefinitions )
    {
        List<String> pathDefs = new ArrayList<String>( Arrays.asList( pathDefinitions ) );
        for ( Path path : paths )
        {
            String pathDef = getPathDef( path );
            int index = pathDefs.indexOf( pathDef );
            if ( index != -1 )
            {
                pathDefs.remove( index );
            }
            else
            {
                fail( "Unexpected path " + pathDef );
            }
        }
        assertTrue( "Should be empty: " + pathDefs.toString(), pathDefs.isEmpty() );
    }

    private String getPathDef( Path path )
    {
        StringBuilder builder = new StringBuilder();
        for ( Node node : path.getNodes() )
        {
            if ( builder.length() > 0 )
            {
                builder.append( "," );
            }
            builder.append( node.getProperty( SimpleGraphBuilder.KEY_ID ) );
        }
        return builder.toString();
    }
}
