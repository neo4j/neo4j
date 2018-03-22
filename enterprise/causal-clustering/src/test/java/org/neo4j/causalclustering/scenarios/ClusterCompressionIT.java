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

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.compression_implementations;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.causalclustering.helpers.DataCreator.countNodes;
import static org.neo4j.causalclustering.helpers.DataCreator.createLabelledNodesWithProperty;
import static org.neo4j.graphdb.Label.label;

@RunWith( Parameterized.class )
public class ClusterCompressionIT
{
    @Parameterized.Parameter
    public ModifierProtocols modifierProtocol;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> params()
    {
        return Arrays.stream( ModifierProtocols.values() ).map( mp -> new Object[]{mp} ).collect( Collectors.toList() );
    }

    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule()
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfReadReplicas( 3 )
                    .withTimeout( 1000, SECONDS );

    @Test
    public void shouldReplicateWithCompression() throws Exception
    {
        // given
        clusterRule
                .withSharedCoreParam( compression_implementations, modifierProtocol.implementation() )
                .withSharedReadReplicaParam( compression_implementations, modifierProtocol.implementation() );

        Cluster cluster = clusterRule.startCluster();

         // when
        int numberOfNodes = 10;
        CoreClusterMember leader = createLabelledNodesWithProperty( cluster, numberOfNodes, label( "Foo" ),
                () -> Pair.of( "foobar", format( "baz_bat%s", UUID.randomUUID() ) ) );

        // then
        assertEquals( numberOfNodes, countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
    }
}
