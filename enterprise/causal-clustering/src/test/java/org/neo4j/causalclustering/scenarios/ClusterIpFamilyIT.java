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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.helpers.DataCreator;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.causalclustering.discovery.IpFamily.IPV4;
import static org.neo4j.causalclustering.discovery.IpFamily.IPV6;
import static org.neo4j.causalclustering.helpers.DataCreator.countNodes;
import static org.neo4j.causalclustering.scenarios.DiscoveryServiceType.HAZELCAST;
import static org.neo4j.causalclustering.scenarios.DiscoveryServiceType.SHARED;

@RunWith( Parameterized.class )
public class ClusterIpFamilyIT
{

    @Parameterized.Parameters( name = "{0} {1} useWildcard={2}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]{
                {SHARED, IPV4, false},
                {SHARED, IPV6, true},

                {HAZELCAST, IPV4, false},
                {HAZELCAST, IPV6, false},

                {HAZELCAST, IPV4, true},
                {HAZELCAST, IPV6, true},
        } );
    }

    public ClusterIpFamilyIT( DiscoveryServiceType discoveryServiceType, IpFamily ipFamily, boolean useWildcard )
    {
        clusterRule.withDiscoveryServiceType( discoveryServiceType );
        clusterRule.withIpFamily( ipFamily ).useWildcard( useWildcard );
    }

    @Rule
    public final ClusterRule clusterRule = new ClusterRule()
            .withNumberOfCoreMembers( 3 )
            .withNumberOfReadReplicas( 3 );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldSetupClusterWithIPv6() throws Exception
    {
        // given
        int numberOfNodes = 10;

        // when
        CoreClusterMember leader = DataCreator.createEmptyNodes( cluster, numberOfNodes );

        // then
        assertEquals( numberOfNodes, countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
        dataMatchesEventually( leader, cluster.readReplicas() );
    }
}
