package org.neo4j.graphalgo.path;

import org.junit.Test;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.TraversalFactory;

import common.Neo4jAlgoTestCase;

public class TestSingleStepShortestPath extends Neo4jAlgoTestCase
{
    protected PathFinder instantiatePathFinder( int maxDepth )
    {
        return new ShortestPath( graphDb, maxDepth,
                TraversalFactory.expanderForTypes( MyRelTypes.R1, Direction.BOTH ) );
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
        Iterable<Path> paths = finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
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
        Iterable<Path> paths =
                finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
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
        Iterable<Path> paths = singleStepFinder.findAllPaths( graph.getNode( "s" ),
                graph.getNode( "t" ) );
        assertPaths( paths, "s,1,2,t", "s,1,4,t", "s,3,2,t", "s,3,4,t" );

        PathFinder finder = instantiatePathFinder( 3 );
        paths = finder.findAllPaths( graph.getNode( "s" ), graph.getNode( "t" ) );
        assertPaths( paths, "s,1,2,t", "s,1,4,t", "s,3,2,t", "s,3,4,t" );
    }
    
    @Test
    public void testDirectedFinder()
    {
        // Layout:
        // 
        // (a)->(b)->(c)->(d)->(e)->(f)-------\
        //    \                                v
        //     >(g)->(h)->(i)->(j)->(k)->(l)->(m)
        //
        graph.makeEdgeChain( "a,b,c,d,e,f,m" );
        graph.makeEdgeChain( "a,g,h,i,j,k,l,m" );
        
        PathFinder finder = new ShortestPath( graphDb, 4,
                TraversalFactory.expanderForTypes( MyRelTypes.R1, Direction.OUTGOING ) );
        assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "j" ) ),
                "a,g,h,i,j" );
    }
}
