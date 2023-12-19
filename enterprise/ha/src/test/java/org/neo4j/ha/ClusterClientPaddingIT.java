/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterRule;

import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsJoined;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterWithAdditionalClients;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesMembers;

public class ClusterClientPaddingIT
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule()
            .withSharedSetting( ClusterSettings.heartbeat_interval, "1s" )
            .withSharedSetting( ClusterSettings.heartbeat_timeout, "10s" );

    private ManagedCluster cluster;

    @Before
    public void setUp()
    {
        cluster = clusterRule.withCluster( clusterWithAdditionalClients( 2, 1 ) )
                .withAvailabilityChecks( masterAvailable(), masterSeesMembers( 3 ), allSeesAllAsJoined() )
                .startCluster();
    }

    @Test
    public void additionalClusterClientCanHelpBreakTiesWhenMasterIsShutDown()
    {
        HighlyAvailableGraphDatabase sittingMaster = cluster.getMaster();
        cluster.shutdown( sittingMaster );
        cluster.await( masterAvailable( sittingMaster ) );
    }

    @Test
    public void additionalClusterClientCanHelpBreakTiesWhenMasterFails()
    {
        HighlyAvailableGraphDatabase sittingMaster = cluster.getMaster();
        cluster.fail( sittingMaster );
        cluster.await( masterAvailable( sittingMaster ) );
    }
}
