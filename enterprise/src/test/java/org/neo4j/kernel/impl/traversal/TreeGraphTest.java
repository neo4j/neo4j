package org.neo4j.kernel.impl.traversal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.kernel.Traversal;

public class TreeGraphTest extends AbstractTestBase
{
    /*
     *                     (1)
     *               ------ | ------
     *             /        |        \
     *           (2)       (3)       (4)
     *          / | \     / | \     / | \
     *        (5)(6)(7) (8)(9)(A) (B)(C)(D)
     */
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
        for ( Path path : traverser )
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
        assertLevels( traverser, levels );
    }

    @Test
    public void testDepthFirstTraversalReturnsNodesOnCorrectDepths()
            throws Exception
    {
        Traverser traverser = new TraversalDescriptionImpl().depthFirst().traverse(
                referenceNode() );
        int i = 0;
        for ( Path pos : traverser )
        {
            assertEquals( expectedDepth( i++ ), pos.length() );
        }
        assertEquals( 13, i );
    }

    @Test
    public void testPostorderDepthFirstReturnsDeeperNodesFirst()
    {
        Traverser traverser = new TraversalDescriptionImpl().order(
                Traversal.postorderDepthFirstSelector() ).traverse(
                        referenceNode() );
        int i = 0;
        List<String> encounteredNodes = new ArrayList<String>();
        for ( Path pos : traverser )
        {
            encounteredNodes.add( (String) pos.endNode().getProperty( "name" ) );
            assertEquals( expectedDepth( ( 12 - i++ ) ), pos.length() );
        }
        assertEquals( 13, i );

        assertTrue( encounteredNodes.indexOf( "5" ) < encounteredNodes.indexOf( "2" ) );
        assertTrue( encounteredNodes.indexOf( "6" ) < encounteredNodes.indexOf( "2" ) );
        assertTrue( encounteredNodes.indexOf( "7" ) < encounteredNodes.indexOf( "2" ) );
        assertTrue( encounteredNodes.indexOf( "8" ) < encounteredNodes.indexOf( "3" ) );
        assertTrue( encounteredNodes.indexOf( "9" ) < encounteredNodes.indexOf( "3" ) );
        assertTrue( encounteredNodes.indexOf( "A" ) < encounteredNodes.indexOf( "3" ) );
        assertTrue( encounteredNodes.indexOf( "B" ) < encounteredNodes.indexOf( "4" ) );
        assertTrue( encounteredNodes.indexOf( "C" ) < encounteredNodes.indexOf( "4" ) );
        assertTrue( encounteredNodes.indexOf( "D" ) < encounteredNodes.indexOf( "4" ) );
        assertTrue( encounteredNodes.indexOf( "2" ) < encounteredNodes.indexOf( "1" ) );
        assertTrue( encounteredNodes.indexOf( "3" ) < encounteredNodes.indexOf( "1" ) );
        assertTrue( encounteredNodes.indexOf( "4" ) < encounteredNodes.indexOf( "1" ) );
    }

    @Test
    public void testPostorderBreadthFirstReturnsDeeperNodesFirst()
    {
        Traverser traverser = new TraversalDescriptionImpl().order(
                Traversal.postorderBreadthFirstSelector() ).traverse(
                        referenceNode() );
        Stack<Set<String>> levels = new Stack<Set<String>>();
        levels.push( new HashSet<String>( Arrays.asList( "1" ) ) );
        levels.push( new HashSet<String>( Arrays.asList( "2", "3", "4" ) ) );
        levels.push( new HashSet<String>( Arrays.asList( "5", "6", "7", "8",
                "9", "A", "B", "C", "D" ) ) );
        assertLevels( traverser, levels );
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
