package org.neo4j.graphalgo.path;

import static org.junit.Assert.assertNotNull;

import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.impl.path.ExactDepthPathFinder;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.Traversal;

import common.Neo4jAlgoTestCase;

public class TestExactDepthPathFinder extends Neo4jAlgoTestCase
{
    @Before
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
        return new ExactDepthPathFinder( Traversal.expanderForAllTypes(), 4, 4 );
    }
    
    @Test
    public void testSingle()
    {
        PathFinder<Path> finder = newFinder();
        Path path = finder.findSinglePath( graph.getNode( "SOURCE" ), graph.getNode( "TARGET" ) );
        assertNotNull( path );
        assertPathDef( path, "SOURCE", "z", "9", "0", "TARGET" ); 
    }

    @Test
    public void testAll()
    {
        assertPaths( newFinder().findAllPaths( graph.getNode( "SOURCE" ), graph.getNode( "TARGET" ) ),
                "SOURCE,z,9,0,TARGET", "SOURCE,SUPER,r,SPIDER,TARGET" );
    }
}
