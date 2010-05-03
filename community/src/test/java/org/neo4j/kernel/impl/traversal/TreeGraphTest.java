package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Position;
import org.neo4j.graphdb.traversal.Traverser;

public class TreeGraphTest extends AbstractTestBase
{
    private static final String[] THE_WORLD_AS_WE_KNOWS_IT = new String[] {
            "1 TO 2", "1 TO 3", "1 TO 4", "2 TO 5", "2 TO 6", "2 TO 7",
            "3 TO 8", "3 TO 9", "3 TO A", "4 TO B", "4 TO C", "4 TO D", };

    @BeforeClass
    public static void setupGraph()
    {
        createGraph( THE_WORLD_AS_WE_KNOWS_IT );
    }

    @Test
    public void nodesIteratorReturnAllNodes() throws Exception
    {
        Traverser traverser = new TraversalDescriptionImpl().traverse( referenceNode() );
        int count = 0;
        for ( Node node : traverser.nodes() )
        {
            assertNotNull( "returned nodes should not be null. node #"
                           + count, node );
            count++;
        }
        assertEquals( 13, count );
    }

    @Test
    public void relationshipsIteratorReturnAllNodes() throws Exception
    {
        Traverser traverser = new TraversalDescriptionImpl().traverse( referenceNode() );
        int count = 0;
        for ( Relationship relationship : traverser.relationships() )
        {
            assertNotNull(
                    "returned relationships should not be. relationship #"
                            + count, relationship );
            count++;
        }
        assertEquals( 12, count );
    }

    @Test
    public void pathsIteratorReturnAllNodes() throws Exception
    {
        Traverser traverser = new TraversalDescriptionImpl().traverse( referenceNode() );
        int count = 0;
        for ( Path path : traverser.paths() )
        {
            assertNotNull( "returned paths should not be null. path #"
                           + count, path );
            count++;
        }
        assertEquals( 13, count );
    }

    @Test
    public void testBreadthFirst() throws Exception
    {
        Traverser traverser = new TraversalDescriptionImpl().breadthFirst().traverse(
                referenceNode() );
        Stack<Set<String>> levels = new Stack<Set<String>>();
        levels.push( new HashSet<String>( Arrays.asList( "5", "6", "7", "8",
                "9", "A", "B", "C", "D" ) ) );
        levels.push( new HashSet<String>( Arrays.asList( "2", "3", "4" ) ) );
        levels.push( new HashSet<String>( Arrays.asList( "1" ) ) );
        Set<String> current = levels.pop();
        for ( Position position : traverser )
        {
            String nodeName = (String) position.node().getProperty( "name" );
            if ( current.isEmpty() )
            {
                current = levels.pop();
            }
            assertTrue( "Should not contain node (" + nodeName
                        + ") at level " + ( 3 - levels.size() ),
                    current.remove( nodeName ) );
        }

        assertTrue( "Should have no more levels", levels.isEmpty() );
        assertTrue( "Should be empty", current.isEmpty() );
    }

    @Test
    public void testDepthFirstTraversalReturnsNodesOnCorrectDepths()
            throws Exception
    {
        Traverser traverser = new TraversalDescriptionImpl().depthFirst().traverse(
                referenceNode() );
        int i = 0;
        for ( Position pos : traverser )
        {
            assertEquals( expectedDepth( i++ ), pos.depth() );
        }
        assertEquals( 13, i );
    }

    private int expectedDepth( int i )
    {
        assertTrue( i < 13 );
        if ( i == 0 )
        {
            return 0;
        }
        else if ( ( i - 1 ) % 4 == 0 )
        {
            return 1;
        }
        else
        {
            return 2;
        }
    }
}
