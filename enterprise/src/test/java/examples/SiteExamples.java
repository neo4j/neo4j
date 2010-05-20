package examples;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphalgo.EstimateEvaluator;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphalgo.centrality.BetweennessCentrality;
import org.neo4j.graphalgo.path.AStar;
import org.neo4j.graphalgo.path.ShortestPath;
import org.neo4j.graphalgo.path.WeightedPath;
import org.neo4j.graphalgo.shortestpath.Dijkstra;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPath;
import org.neo4j.graphalgo.shortestpath.SingleSourceShortestPathBFS;
import org.neo4j.graphalgo.shortestpath.SingleSourceSingleSinkShortestPath;
import org.neo4j.graphalgo.util.DoubleAdder;
import org.neo4j.graphalgo.util.DoubleComparator;
import org.neo4j.graphalgo.util.DoubleEvaluator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.TraversalFactory;

public class SiteExamples
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
        graphDb = new EmbeddedGraphDatabase( "target/var/examples" );
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
    // START SNIPPET: shortestPathUsage
    public void shortestPathExample()
    {
        Node startNode = graphDb.createNode();
        Node middleNode1 = graphDb.createNode();
        Node middleNode2 = graphDb.createNode();
        Node middleNode3 = graphDb.createNode();
        Node endNode = graphDb.createNode();
        createRelationshipsBetween( startNode, middleNode1, endNode );
        createRelationshipsBetween( startNode, middleNode2, middleNode3, endNode );
        
        // Will find the shortest path between startNode and endNode via
        // "MY_TYPE" relationships (irregardless of their directions)
        PathFinder finder = new ShortestPath( graphDb, 15,
                TraversalFactory.expanderForTypes( ExampleTypes.MY_TYPE, Direction.BOTH ) );
        Iterable<Path> paths = finder.findAllPaths( startNode, endNode );
    }
    // END SNIPPET: shortestPathUsage
    
    private void createRelationshipsBetween( Node... nodes )
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
        findShortestPathWithDijkstra( node1, node2 );
    }
    
    // START SNIPPET: dijkstraUsage
    public List<PropertyContainer> findShortestPathWithDijkstra( Node start, Node end )
    {
        // Set up Dijkstra
        SingleSourceSingleSinkShortestPath<Double> sp;
        sp = new Dijkstra<Double>( 0.0, start, end, new DoubleEvaluator(
                "cost" ), new DoubleAdder(), new DoubleComparator(),
                Direction.BOTH, DynamicRelationshipType.withName( "MY_TYPE" ) );

        // Get the cost for the found path
        sp.getCost();

        // Get the path itself
        return sp.getPath();
    }
    // END SNIPPET: dijkstraUsage
    
    private Node createNode( Object... properties )
    {
        return setProperties( graphDb.createNode(), properties );
    }
    
    private <T extends PropertyContainer> T setProperties( T entity, Object[] properties )
    {
        for ( int i = 0; i < properties.length; i++ )
        {
            String key = properties[i++].toString();
            Object value = properties[i];
            entity.setProperty( key, value );
        }
        return entity;
    }

    private Relationship createRelationship( Node start, Node end,
            Object... properties )
    {
        return setProperties( start.createRelationshipTo( end, ExampleTypes.MY_TYPE ),
                properties );
    }
    
    @Test
    // START SNIPPET: astarUsage
    public void astarExample()
    {
        Node nodeA = createNode( "name", "A", "x", 0d, "y", 0d );
        Node nodeB = createNode( "name", "B", "x", 2d, "y", 1d );
        Node nodeC = createNode( "name", "C", "x", 7d, "y", 0d );
        Relationship relAB = createRelationship( nodeA, nodeB, "length", 2d );
        Relationship relBC = createRelationship( nodeB, nodeC, "length", 3d );
        Relationship relAC = createRelationship( nodeA, nodeC, "length", 10d );
        
        EstimateEvaluator<Double> estimateEvaluator = new EstimateEvaluator<Double>()
        {
            public Double getCost( Node node, Node goal )
            {
                double dx = (Double) node.getProperty( "x" ) - (Double) goal.getProperty( "x" );
                double dy = (Double) node.getProperty( "y" ) - (Double) goal.getProperty( "y" );
                double result = Math.sqrt( Math.pow( dx, 2 ) + Math.pow( dy, 2 ) );
                return result;
            }
        };
        PathFinder<WeightedPath> astar = new AStar( graphDb, TraversalFactory.expanderForAllTypes(),
                new DoubleEvaluator( "length" ), estimateEvaluator );
        Path path = astar.findSinglePath( nodeA, nodeC );
    }
    // END SNIPPET: astarUsage
    
    @Test
    public void centralityUsage()
    {
        Node targetNode = graphDb.createNode();
        getCentrality( targetNode, new HashSet<Node>( Arrays.asList( targetNode ) ) );
    }
    
    // START SNIPPET: centralityUsage
    public double getCentrality( Node targetNode, Set<Node> nodeSet )
    {
        // Set up shortest path algorithm.
        // Observe that we don't need to specify a start node.
        SingleSourceShortestPath<Integer> singleSourceShortestPath;
        singleSourceShortestPath = new SingleSourceShortestPathBFS(
            null, Direction.BOTH, ExampleTypes.MY_TYPE );
                
        // Set up betweenness centrality algorithm.
        BetweennessCentrality<Integer> betweennessCentrality;
        betweennessCentrality = new BetweennessCentrality<Integer>(
            singleSourceShortestPath, nodeSet );

        // Get centrality value for a node.
        return betweennessCentrality.getCentrality( targetNode );
    }
    // END SNIPPET: centralityUsage
}
