/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.test.causalclustering;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.HashSet;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.HttpConnector;
import org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

public class ClusterRuleIT
{
    private static final int NumberOfPortsUsedByCoreMember = 6;
    private static final int NumberOfPortsUsedByReadReplica = 4;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    @Test
    public void shouldAssignPortsToMembersAutomatically() throws Exception
    {
        Cluster cluster = clusterRule.withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 5 ).startCluster();

        int numberOfCoreMembers = cluster.coreMembers().size();
        assertThat( numberOfCoreMembers, is( 3 ) );
        int numberOfReadReplicas = cluster.readReplicas().size();
        assertThat( numberOfReadReplicas, is( 5 ) );

        Set<Integer> portsUsed = gatherPortsUsed( cluster );

        // so many for core members, so many for read replicas, all unique
        assertThat( portsUsed.size(), is(
                numberOfCoreMembers * NumberOfPortsUsedByCoreMember +
                        numberOfReadReplicas * NumberOfPortsUsedByReadReplica ) );
    }

    private Set<Integer> gatherPortsUsed( Cluster cluster )
    {
        Set<Integer> portsUsed = new HashSet<>();

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.discovery_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.transaction_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, CausalClusteringSettings.raft_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, OnlineBackupSettings.online_backup_server.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, new BoltConnector( "bolt" ).listen_address.name() ) );
            portsUsed.add( getPortFromSetting( coreClusterMember, new HttpConnector( "http" ).listen_address.name() ) );
        }

        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            portsUsed.add( getPortFromSetting( readReplica, CausalClusteringSettings.transaction_listen_address.name() ) );
            portsUsed.add( getPortFromSetting( readReplica, OnlineBackupSettings.online_backup_server.name() ) );
            portsUsed.add( getPortFromSetting( readReplica, new BoltConnector( "bolt" ).listen_address.name() ) );
            portsUsed.add( getPortFromSetting( readReplica, new HttpConnector( "http" ).listen_address.name() ) );
        }
        return portsUsed;
    }

    private int getPortFromSetting( ClusterMember coreClusterMember, String settingName )
    {
        String setting = coreClusterMember.settingValue( settingName );
        return Integer.valueOf( setting.split( ":" )[1] );
    }
}
