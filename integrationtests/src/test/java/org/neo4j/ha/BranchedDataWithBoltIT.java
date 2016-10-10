package org.neo4j.ha;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;

public class BranchedDataWithBoltIT
{
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory( getClass() );

    @Test
    public void mustHandleBranchedDataWithBoltAvailable() throws Throwable
    {
        // GIVEN
        ClusterManager clusterManager = new ClusterManager.Builder( dir.cleanDirectory( "dbs" ) )
                .withCluster( clusterOfSize( 2 ) ).build();
        clusterManager.start();
        ClusterManager.ManagedCluster cluster = clusterManager.getCluster();
        cluster.await( allSeesAllAsAvailable() );
        createNode( cluster.getMaster(), "A" );
        cluster.sync();

        // WHEN
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        File slaveStoreDir = new File( slave.getStoreDir() );
        ClusterManager.RepairKit starter = cluster.shutdown( slave );
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        createNode( master, "B1" );
        createNode( master, "C" );

        // Create node offline
        GraphDatabaseService single = new TestGraphDatabaseFactory().newEmbeddedDatabase( slaveStoreDir );
        createNode( single, "B2" );
        single.shutdown();
        slave = starter.repair();

        // THEN
        cluster.await( allSeesAllAsAvailable() );
        slave.beginTx().close();
    }

    @SuppressWarnings( "unchecked" )
    private void createNode( GraphDatabaseService db, String name, Listener<Node>... additional )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = createNamedNode( db, name );
            for ( Listener<Node> listener : additional )
            {
                listener.receive( node );
            }
            tx.success();
        }
    }

    private Node createNamedNode( GraphDatabaseService db, String name )
    {
        Node node = db.createNode();
        node.setProperty( "name", name );
        return node;
    }
}
