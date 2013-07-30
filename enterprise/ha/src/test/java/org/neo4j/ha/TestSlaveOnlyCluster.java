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
package org.neo4j.ha;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import org.neo4j.cluster.InstanceId;
import org.neo4j.cluster.client.ClusterClient;
import org.neo4j.cluster.protocol.cluster.ClusterListener;
import org.neo4j.cluster.protocol.heartbeat.HeartbeatListener;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.test.ha.ClusterManager.fromXml;

public class TestSlaveOnlyCluster
{
    @Test
    public void testMasterElectionAfterMasterRecoversInSlaveOnlyCluster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).directory( "testCluster", true ), MapUtil.stringMap(),
                MapUtil.<Integer, Map<String, String>>genericMap( 2, MapUtil.stringMap( HaSettings.slave_only.name(), "true" ),
                                                                  3, MapUtil.stringMap( HaSettings.slave_only.name(), "true" )) );


        try
        {
            clusterManager.start();

            final CountDownLatch failedLatch = new CountDownLatch( 2 );
            final CountDownLatch electedLatch = new CountDownLatch( 2 );
            HeartbeatListener masterDownListener = new HeartbeatListener()
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
            };

            for ( HighlyAvailableGraphDatabase highlyAvailableGraphDatabase : clusterManager.getDefaultCluster().getAllMembers() )
            {
                if (!highlyAvailableGraphDatabase.isMaster())
                {
                    highlyAvailableGraphDatabase.getDependencyResolver().resolveDependency( ClusterClient.class ).addHeartbeatListener( masterDownListener );

                    highlyAvailableGraphDatabase.getDependencyResolver().resolveDependency( ClusterClient.class ).addClusterListener( new ClusterListener.Adapter()
                    {
                        @Override
                        public void elected( String role, InstanceId electedMember, URI availableAtUri )
                        {
                            electedLatch.countDown();
                        }
                    } );
                }
            }

            HighlyAvailableGraphDatabase master = clusterManager.getDefaultCluster().getMaster();
            ClusterManager.RepairKit repairKit = clusterManager.getDefaultCluster().fail( master );

            failedLatch.await();

            repairKit.repair();

            electedLatch.await();

            HighlyAvailableGraphDatabase slaveDatabase = clusterManager.getDefaultCluster().getAnySlave(  );
            Transaction tx = slaveDatabase.beginTx();
            Node node = slaveDatabase.createNode();
            node.setProperty( "foo", "bar" );
            long nodeId = node.getId();
            tx.success();
            tx.finish();

            Transaction transaction = master.beginTx();
            try
            {
                assertThat( master.getNodeById( nodeId ).getProperty( "foo" ).toString(), equalTo( "bar" ) );
            }
            finally
            {
                transaction.finish();
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
        ClusterManager clusterManager = new ClusterManager( fromXml( getClass().getResource( "/threeinstances.xml" ).toURI() ),
                TargetDirectory.forTest( getClass() ).directory( "testCluster", true ), MapUtil.stringMap(),
                MapUtil.<Integer, Map<String, String>>genericMap( 1, MapUtil.stringMap( HaSettings.slave_only.name(), "true" ),
                                       2, MapUtil.stringMap( HaSettings.slave_only.name(), "true" )) );

        try
        {
            clusterManager.start();

            HighlyAvailableGraphDatabase master = clusterManager.getDefaultCluster().getMaster();
            assertThat( clusterManager.getDefaultCluster().getServerId( master ), CoreMatchers.equalTo( 3 ) );
        }
        finally
        {
            clusterManager.stop();
        }
    }
}
