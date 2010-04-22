package org.neo4j.graphdb.traversal;

import static org.junit.Assert.assertEquals;

import java.util.Collection;

import org.junit.After;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import common.AbstractTestBase;
import common.StandardGraphs;

@Ignore
public class ShortestPaths extends AbstractTestBase
{
    /* Implementation of shortest path using these traversals. */
    private Collection<Path> shortestPaths( Node start, Node end )
    {
        // TODO Auto-generated method stub
        return null;
    }

    @After
    public void cleanup()
    {
        removeAllNodes( false );
    }

    @Test
    public void crossPathsGraphHasFourShortestPathsOfLengthThree()
            throws Exception
    {
        Node end = createGraph( StandardGraphs.CROSS_PATHS_GRAPH );
        Collection<Path> paths = shortestPaths( referenceNode(), end );
        assertEquals( 4, paths.size() );
        for ( Path path : paths )
        {
            assertEquals( 3, path.length() );
        }
    }

    @Test
    public void smallCircleHasTwoShortestPathsOfLengthOne() throws Exception
    {
        Node end = createGraph( StandardGraphs.SMALL_CIRCLE );
        Collection<Path> paths = shortestPaths( referenceNode(), end );
        assertEquals( 2, paths.size() );
        for ( Path path : paths )
        {
            assertEquals( 1, path.length() );
        }
    }
}
