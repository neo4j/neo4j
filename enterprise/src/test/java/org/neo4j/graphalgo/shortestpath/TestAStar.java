package org.neo4j.graphalgo.shortestpath;

import org.junit.Test;
import org.neo4j.graphalgo.shortestpath.std.DoubleEvaluator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.TraversalFactory;

import common.Neo4jAlgoTestCase;

public class TestAStar extends Neo4jAlgoTestCase
{
    @Test
    public void testSimplest()
    {
        Node nodeA = graph.makeNode( "A", "x", 0d, "y", 0d );
        Node nodeB = graph.makeNode( "B", "x", 2d, "y", 1d );
        Node nodeC = graph.makeNode( "C", "x", 7d, "y", 0d );
        Relationship relAB = graph.makeEdge( "A", "B", "length", 2d );
        Relationship relBC = graph.makeEdge( "B", "C", "length", 3d );
        Relationship relAC = graph.makeEdge( "A", "C", "length", 10d );

        EstimateEvaluator<Double> estimateEvaluator = new EstimateEvaluator<Double>()
        {
            public Double getCost( Node node, Node goal )
            {
                double dx = (Double) node.getProperty( "x" )
                            - (Double) goal.getProperty( "x" );
                double dy = (Double) node.getProperty( "y" )
                            - (Double) goal.getProperty( "y" );
                double result = Math.sqrt( Math.pow( dx, 2 ) + Math.pow( dy, 2 ) );
                return result;
            }
        };
        AStar astar = new AStar( graphDb, TraversalFactory.expanderForAllTypes(),
                new DoubleEvaluator( "length" ), estimateEvaluator );
        Path path = astar.findSinglePath( nodeA, nodeC );
        assertPath( path, nodeA, nodeB, nodeC );
    }
}
