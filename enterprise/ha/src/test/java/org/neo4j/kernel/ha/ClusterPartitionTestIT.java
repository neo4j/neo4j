/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.function.Predicate;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.NetworkFlag;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;

public class ClusterPartitionTestIT
{
    @Rule
    public LoggerRule logger = new LoggerRule();
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void isolatedMasterShouldRemoveSelfFromCluster() throws Throwable
    {
        int clusterSize = 3;

        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) )
                .withProvider( ClusterManager.clusterOfSize( clusterSize ) )
                .withSharedConfig( stringMap(
                        ClusterSettings.heartbeat_interval.name(), "1" ) )
                .build();

        try
        {
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getDefaultCluster();

            cluster.await( allSeesAllAsAvailable() );
            cluster.await( masterAvailable() );


            HighlyAvailableGraphDatabase oldMaster = cluster.getMaster();

            cluster.fail( oldMaster, NetworkFlag.values() );

            cluster.await( oldMasterEvicted( oldMaster ), 20 );
        }
        finally
        {
            manager.safeShutdown();
        }
    }

    private Predicate<ClusterManager.ManagedCluster> oldMasterEvicted( final HighlyAvailableGraphDatabase oldMaster )
    {
        return new Predicate<ClusterManager.ManagedCluster>()
        {
            @Override
            public boolean test( ClusterManager.ManagedCluster managedCluster )
            {

                InstanceId oldMasterServerId = managedCluster.getServerId( oldMaster );

                Iterable<HighlyAvailableGraphDatabase> members = managedCluster.getAllMembers();
                for ( HighlyAvailableGraphDatabase member : members )
                {
                    if ( oldMasterServerId.equals( managedCluster.getServerId( member ) ) )
                    {
                        if ( member.role().equals( "UNKNOWN" ) )
                        {
                            return true;
                        }
                    }
                }
                return false;

            }
        };
    }
}
