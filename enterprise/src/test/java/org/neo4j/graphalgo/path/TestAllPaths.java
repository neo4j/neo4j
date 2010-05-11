package org.neo4j.graphalgo.path;

import org.junit.Test;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Path;
import org.neo4j.kernel.TraversalFactory;

import common.Neo4jAlgoTestCase;

public class TestAllPaths extends Neo4jAlgoTestCase
{
    protected PathFinder instantiatePathFinder( int maxDepth )
    {
        return new AllPaths( maxDepth, TraversalFactory.expanderForAllTypes() );
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

        PathFinder finder = instantiatePathFinder( 10 );
        Iterable<Path> paths = finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "e" ) );
        assertPaths( paths, "a,b,c,e", "a,b,c,e", "a,b,d,c,e", "a,b,c,d,b,c,e", "a,b,c,d,b,c,e",
                "a,b,c,b,d,c,e", "a,b,c,b,d,c,e", "a,b,d,c,b,c,e", "a,b,d,c,b,c,e" );
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
        
        PathFinder finder = instantiatePathFinder( 10 );
        Iterable<Path> paths = finder.findAllPaths( graph.getNode( "a" ), graph.getNode( "d" ) );
        assertPaths( paths, "a,b,c,d", "a,b,c,d", "a,b,c,d",
                "a,b,c,b,c,d", "a,b,c,b,c,d", "a,b,c,b,c,d", "a,b,c,b,c,d", "a,b,c,b,c,d", "a,b,c,b,c,d" );
    }
}
