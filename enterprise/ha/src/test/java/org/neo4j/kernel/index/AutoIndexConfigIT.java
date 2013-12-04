/**
 * Copyright (c) 2002-2013 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.index;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.hasItem;
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
        startCluster( 3 );
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
