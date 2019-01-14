/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
