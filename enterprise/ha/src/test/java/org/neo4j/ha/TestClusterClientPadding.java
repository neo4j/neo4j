/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.test.ha.ClusterRule;

import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsJoined;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterWithAdditionalClients;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesMembers;

public class TestClusterClientPadding
{
    @Rule
    public ClusterRule clusterRule = new ClusterRule( getClass() );
    private ManagedCluster cluster;

    @Before
    public void setUp() throws Throwable
    {
        cluster = clusterRule.withProvider( clusterWithAdditionalClients( 2, 1 ) )
                .withAvailabilityChecks( masterAvailable(), masterSeesMembers( 3 ), allSeesAllAsJoined() )
                .startCluster();
    }

    @Test
    public void additionalClusterClientCanHelpBreakTiesWhenMasterIsShutDown() throws Throwable
    {
        HighlyAvailableGraphDatabase sittingMaster = cluster.getMaster();
        cluster.shutdown( sittingMaster );
        cluster.await( masterAvailable( sittingMaster ) );
    }

    @Test
    public void additionalClusterClientCanHelpBreakTiesWhenMasterFails() throws Throwable
    {
        HighlyAvailableGraphDatabase sittingMaster = cluster.getMaster();
        cluster.fail( sittingMaster );
        cluster.await( masterAvailable( sittingMaster ) );
    }
}
