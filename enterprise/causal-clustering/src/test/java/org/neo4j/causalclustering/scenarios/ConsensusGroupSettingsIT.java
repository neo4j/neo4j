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
package org.neo4j.causalclustering.scenarios;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

public class ConsensusGroupSettingsIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 5 )
            .withNumberOfReadReplicas( 0 )
            .withInstanceCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_formation, value -> "5" )
            .withInstanceCoreParam( CausalClusteringSettings.minimum_core_cluster_size_at_runtime,value -> "3" )
            .withInstanceCoreParam( CausalClusteringSettings.leader_election_timeout, value -> "1s" )
            .withTimeout( 1000, SECONDS );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldNotAllowTheConsensusGroupToDropBelowMinimumConsensusGroupSize() throws Exception
    {
        // given
        int numberOfCoreSeversToRemove = 3;

        cluster.awaitLeader();

        // when
        for ( int i = 0; i < numberOfCoreSeversToRemove; i++ )
        {
            cluster.removeCoreMember( cluster.getMemberWithRole( Role.LEADER ) );
            cluster.awaitLeader( 30, SECONDS );
        }

        // then

        assertEquals(3, cluster.coreMembers().iterator().next().raft().replicationMembers().size());
    }
}
