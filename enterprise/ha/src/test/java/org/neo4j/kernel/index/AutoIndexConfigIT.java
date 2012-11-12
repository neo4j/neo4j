package org.neo4j.kernel.index;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.internal.matchers.IsCollectionContaining.hasItem;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;

import org.junit.After;
import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.index.AutoIndexer;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

public class AutoIndexConfigIT
{

    private static final TargetDirectory dir = TargetDirectory.forTest( AutoIndexConfigIT.class );
    private ClusterManager.ManagedCluster cluster;
    private ClusterManager clusterManager;

    public void startCluster( int size ) throws Throwable
    {
        clusterManager = new ClusterManager( clusterOfSize( size ), dir.directory( "dbs", true ), MapUtil.stringMap() )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, String clusterName, int serverId )
            {
                builder.setConfig( "jmx.port", "" + (9912+serverId) );
                builder.setConfig( HaSettings.ha_server, ":" + (1136+serverId) );
            }
        };
        clusterManager.start();
        cluster = clusterManager.getDefaultCluster();
    }

    @After
    public void stopCluster() throws Throwable
    {
        clusterManager.stop();
    }

    @Test
    public void programmaticConfigShouldSurviveMasterSwitches() throws Throwable
    {
        String propertyToIndex = "programmatic-property";

        // Given
        startCluster( 2 );
        HighlyAvailableGraphDatabase originalMaster = cluster.getMaster();

        AutoIndexer<Node> originalAutoIndex = originalMaster.index().getNodeAutoIndexer();
        originalAutoIndex.setEnabled( true );
        originalAutoIndex.startAutoIndexingProperty( propertyToIndex );

        // When
        ClusterManager.RepairKit originalMasterRepairKit = cluster.shutdown( originalMaster );
        cluster.await( masterAvailable() );
        originalMasterRepairKit.repair(); // Bring the original master back as a slave

        // Then
        AutoIndexer<Node> newAutoIndex = originalMaster.index().getNodeAutoIndexer();

        assertThat(newAutoIndex.isEnabled(), is(true));
        assertThat( newAutoIndex.getAutoIndexedProperties(), hasItem( propertyToIndex ) );
    }

}
