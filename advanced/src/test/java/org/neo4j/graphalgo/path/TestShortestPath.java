package org.neo4j.graphalgo.path;

import org.junit.Test;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.path.ShortestPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipExpander;
import org.neo4j.kernel.Traversal;

import common.Neo4jAlgoTestCase;

public class TestShortestPath extends Neo4jAlgoTestCase
{
    protected PathFinder<Path> instantiatePathFinder( int maxDepth )
    {
        return instantiatePathFinder( Traversal.expanderForTypes( MyRelTypes.R1,
                Direction.BOTH ), maxDepth );
    }
    
    protected PathFinder<Path> instantiatePathFinder( RelationshipExpander expander, int maxDepth )
    {
        return GraphAlgoFactory.shortestPath(
                expander, maxDepth );
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

        PathFinder<Path> finder = instantiatePathFinder( 1 );
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

        PathFinder<Path> finder = instantiatePathFinder( 6 );
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
        
        PathFinder<Path> singleStepFinder = instantiatePathFinder( 3 );
        Iterable<Path> paths = singleStepFinder.findAllPaths( graph.getNode( "s" ),
                graph.getNode( "t" ) );
        assertPaths( paths, "s,1,2,t", "s,1,4,t", "s,3,2,t", "s,3,4,t" );

        PathFinder<Path> finder = instantiatePathFinder( 3 );
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
        
        PathFinder<Path> finder = instantiatePathFinder(
                Traversal.expanderForTypes( MyRelTypes.R1, Direction.OUTGOING ), 4 );
        assertPaths( finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "j" ) ),
                "a,g,h,i,j" );
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
        assertPaths( new ShortestPath( 3, expander, true ).findAllPaths( a, k ), "a,c,g,k" );
        assertPaths( new ShortestPath( 4, expander, true ).findAllPaths( a, k ), "a,b,d,j,k" );
        assertPaths( new ShortestPath( 5, expander, true ).findAllPaths( a, k ) );
        assertPaths( new ShortestPath( 6, expander, true ).findAllPaths( a, k ), "a,b,d,h,i,j,k" );
        assertPaths( new ShortestPath( 7, expander, true ).findAllPaths( a, k ), "a,b,e,f,h,i,j,k" );
        assertPaths( new ShortestPath( 8, expander, true ).findAllPaths( a, k ) );
    }
}
