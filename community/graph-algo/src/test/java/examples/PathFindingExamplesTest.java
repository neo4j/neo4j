/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package examples;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.util.Iterator;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.CommonEvaluators;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.WeightedPath;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;

import common.Neo4jAlgoTestCase;

public class PathFindingExamplesTest
{
    private static GraphDatabaseService graphDb;
    private Transaction tx;

    private static enum ExampleTypes implements RelationshipType
    {
        MY_TYPE
    }

    @BeforeClass
    public static void startDb()
    {
        String storeDir = "target/var/examples";
        Neo4jAlgoTestCase.deleteFileOrDirectory( new File( storeDir ) );
        graphDb = new EmbeddedGraphDatabase( storeDir );
    }

    @Before
    public void doBefore()
    {
        tx = graphDb.beginTx();
    }

    @After
    public void doAfter()
    {
        tx.success();
        tx.finish();
    }

    @AfterClass
    public static void shutdownDb()
    {
        graphDb.shutdown();
    }

    @Test
    public void shortestPathExample()
    {
        // START SNIPPET: shortestPathUsage
        Node startNode = graphDb.createNode();
        Node middleNode1 = graphDb.createNode();
        Node middleNode2 = graphDb.createNode();
        Node middleNode3 = graphDb.createNode();
        Node endNode = graphDb.createNode();
        createRelationshipsBetween( startNode, middleNode1, endNode );
        createRelationshipsBetween( startNode, middleNode2, middleNode3, endNode );

        // Will find the shortest path between startNode and endNode via
        // "MY_TYPE" relationships (in OUTGOING direction), like f.ex:
        //
        // (startNode)-->(middleNode1)-->(endNode)
        //
        PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                Traversal.expanderForTypes( ExampleTypes.MY_TYPE, Direction.OUTGOING ), 15 );
        Iterable<Path> paths = finder.findAllPaths( startNode, endNode );
        // END SNIPPET: shortestPathUsage
        Path path = paths.iterator().next();
        assertEquals( 2, path.length() );
        assertEquals( startNode, path.startNode() );
        assertEquals( endNode, path.endNode() );
        Iterator<Node> iterator = path.nodes().iterator();
        iterator.next();
        assertEquals( middleNode1, iterator.next() );
    }

    private void createRelationshipsBetween( final Node... nodes )
    {
        for ( int i = 0; i < nodes.length - 1; i++ )
        {
            nodes[i].createRelationshipTo( nodes[i+1], ExampleTypes.MY_TYPE );
        }
    }

    @Test
    public void dijkstraUsage()
    {
        Node node1 = graphDb.createNode();
        Node node2 = graphDb.createNode();
        Relationship rel = node1.createRelationshipTo( node2, ExampleTypes.MY_TYPE );
        rel.setProperty( "cost", 1d );
        findCheapestPathWithDijkstra( node1, node2 );
    }

    public WeightedPath findCheapestPathWithDijkstra( final Node nodeA, final Node nodeB )
    {
        // START SNIPPET: dijkstraUsage
        PathFinder<WeightedPath> finder = GraphAlgoFactory.dijkstra(
                Traversal.expanderForTypes( ExampleTypes.MY_TYPE, Direction.BOTH ), "cost" );

        WeightedPath path = finder.findSinglePath( nodeA, nodeB );

        // Get the weight for the found path
        path.weight();
        // END SNIPPET: dijkstraUsage
        return path;
    }

    private Node createNode( final Object... properties )
    {
        return setProperties( graphDb.createNode(), properties );
    }

    private <T extends PropertyContainer> T setProperties( final T entity, final Object[] properties )
    {
        for ( int i = 0; i < properties.length; i++ )
        {
            String key = properties[i++].toString();
            Object value = properties[i];
            entity.setProperty( key, value );
        }
        return entity;
    }

    private Relationship createRelationship( final Node start, final Node end,
            final Object... properties )
    {
        return setProperties( start.createRelationshipTo( end, ExampleTypes.MY_TYPE ),
                properties );
    }

    @Test
    public void astarExample()
    {
        // START SNIPPET: astarUsage
        Node nodeA = createNode( "name", "A", "x", 0d, "y", 0d );
        Node nodeB = createNode( "name", "B", "x", 7d, "y", 0d );
        Node nodeC = createNode( "name", "C", "x", 2d, "y", 1d );
        Relationship relAB = createRelationship( nodeA, nodeC, "length", 2d );
        Relationship relBC = createRelationship( nodeC, nodeB, "length", 3d );
        Relationship relAC = createRelationship( nodeA, nodeB, "length", 10d );

        EstimateEvaluator<Double> estimateEvaluator = new EstimateEvaluator<Double>()
        {
            public Double getCost( final Node node, final Node goal )
            {
                double dx = (Double) node.getProperty( "x" ) - (Double) goal.getProperty( "x" );
                double dy = (Double) node.getProperty( "y" ) - (Double) goal.getProperty( "y" );
                double result = Math.sqrt( Math.pow( dx, 2 ) + Math.pow( dy, 2 ) );
                return result;
            }
        };
        PathFinder<WeightedPath> astar = GraphAlgoFactory.aStar(
                Traversal.expanderForAllTypes(),
                CommonEvaluators.doubleCostEvaluator( "length" ), estimateEvaluator );
        WeightedPath path = astar.findSinglePath( nodeA, nodeB );
        // END SNIPPET: astarUsage
        System.out.println( path.weight() );
    }
}
