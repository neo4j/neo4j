package org.neo4j.graphdb.traversal;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Relationship;

import common.AbstractTestBase;

public class TestMultiPruneEvaluators extends AbstractTestBase
{
    @BeforeClass
    public static void setupGraph()
    {
        createGraph( "a to b", "a to c", "a to d", "a to e",
                "b to f", "b to g", "b to h",
                "c to i",
                "d to j", "d to k", "d to l",
                "e to m", "e to n",
                "k to o", "k to p", "k to q", "k to r" );
    }
    
    @Test
    public void testMaxDepthAndCustomPruneEvaluatorCombined()
    {
        TraversalDescription description = new TraversalDescription().filter( ReturnFilter.ALL )
                .prune( new PruneAfterDepth( 1 ) ).prune( new PruneEvaluator()
                {
                    public boolean pruneAfter( Position position )
                    {
                        int counter = 0;
                        for ( Iterator<Relationship> rels = position.node().getRelationships(
                                Direction.OUTGOING ).iterator(); rels.hasNext(); )
                        {
                            counter++;
                            rels.next();
                        }
                        return counter < 3;
                    }
                } );
        Set<String> expectedNodes = new HashSet<String>(
                Arrays.asList( "a", "b", "c", "d", "e" ) );
        for ( Position position : description.traverse( referenceNode() ) )
        {
            String name = (String) position.node().getProperty( "name" );
            assertTrue( name + " shouldn't have been returned", expectedNodes.remove( name ) );
        }
        assertTrue( expectedNodes.isEmpty() );
    }
}
