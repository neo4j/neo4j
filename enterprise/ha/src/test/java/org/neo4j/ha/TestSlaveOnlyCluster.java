/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ha;

import java.io.File;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.fromXml;

public class TestSlaveOnlyCluster
{
    private final TargetDirectory directory = TargetDirectory.forTest( getClass() );
    private static final String PROPERTY = "foo";
    private static final String VALUE = "bar";

    @Test
    public void testMasterElectionAfterMasterRecoversInSlaveOnlyCluster() throws Throwable
    {
        final ClusterManager clusterManager = createCluster( "masterrecovery", 1, 2 );
        try
        {
            clusterManager.start();

            final ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( allSeesAllAsAvailable() );

            final HighlyAvailableGraphDatabase master = cluster.getMaster();
            final CountDownLatch masterFailedLatch = createMasterFailLatch( cluster );

            final ClusterManager.RepairKit repairKit = cluster.fail( master );

            masterFailedLatch.await( 60, TimeUnit.SECONDS );

            repairKit.repair();

            cluster.await( allSeesAllAsAvailable() );

            clusterManager.getDefaultCluster().await( ClusterManager.allSeesAllAsAvailable() );
            long nodeId = createNodeWithPropertyOn( cluster.getAnySlave(), PROPERTY, VALUE );

            try ( Transaction ignore = master.beginTx() )
            {
                assertThat( (String) master.getNodeById( nodeId ).getProperty( PROPERTY ), equalTo( VALUE ) );
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void testMasterElectionAfterSlaveOnlyInstancesStartFirst() throws Throwable
    {
        final ClusterManager clusterManager = createCluster( "slaveonly", 1, 2 );

        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( allSeesAllAsAvailable() );

            assertThat( cluster.getServerId( cluster.getMaster() ), equalTo( new InstanceId( 3 ) ) );
        }
        finally
        {
            clusterManager.stop();
        }
    }

    private ClusterManager createCluster( String dirname, int... slaveIds ) throws URISyntaxException
    {
        final File dir = directory.cleanDirectory( dirname );
        final ClusterManager.Provider provider = fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() );
        final Map<Integer, Map<String, String>> instanceConfig = new HashMap<>( slaveIds.length );
        for ( int slaveId : slaveIds )
        {
            instanceConfig.put( slaveId, MapUtil.stringMap( HaSettings.slave_only.name(), "true" ) );
        }

        return new ClusterManager( provider, dir, MapUtil.stringMap(), instanceConfig );
    }

    private long createNodeWithPropertyOn( HighlyAvailableGraphDatabase db, String property, String value )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            node.setProperty( property, value );

            tx.success();

            return node.getId();
        }
    }

    private CountDownLatch createMasterFailLatch( ClusterManager.ManagedCluster cluster )
    {
        final CountDownLatch failedLatch = new CountDownLatch( 2 );
        for ( HighlyAvailableGraphDatabase db : cluster.getAllMembers() )
        {
            if ( !db.isMaster() )
            {
                db.getDependencyResolver().resolveDependency( ClusterClient.class )
                        .addHeartbeatListener( new HeartbeatListener()
                        {
                            @Override
                            public void failed( InstanceId server )
                            {
                                failedLatch.countDown();
                            }

                            @Override
                            public void alive( InstanceId server )
                            {
                            }
                        } );
            }
        }
        return failedLatch;
    }
}
