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

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.test.causalclustering.ClusterRule;

import static org.neo4j.causalclustering.helpers.DataCreator.createLabelledNodesWithProperty;
import static org.neo4j.causalclustering.scenarios.ReadReplicaToReadReplicaCatchupIT.checkDataHasReplicatedToReadReplicas;
import static org.neo4j.graphdb.Label.label;

public class ReadReplicaHierarchicalCatchupIT
{
    private Map<Integer,String> serverGroups = new HashMap<>();

    @Before
    public void setup()
    {
        serverGroups.put( 0, "NORTH" );
        serverGroups.put( 1, "NORTH" );
        serverGroups.put( 2, "NORTH" );

        serverGroups.put( 3, "EAST" );
        serverGroups.put( 5, "EAST" );

        serverGroups.put( 4, "WEST" );
        serverGroups.put( 6, "WEST" );
    }

    @Rule
    public ClusterRule clusterRule =
            new ClusterRule().withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 )
                    .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
                    .withSharedCoreParam( CausalClusteringSettings.multi_dc_license, "true" )
                    .withSharedReadReplicaParam( CausalClusteringSettings.multi_dc_license, "true" )
                    .withDiscoveryServiceType( DiscoveryServiceType.HAZELCAST );

    @Test
    public void shouldCatchupThroughHierarchy() throws Throwable
    {
        clusterRule = clusterRule
                .withInstanceReadReplicaParam( CausalClusteringSettings.server_groups, id -> serverGroups.get( id ) )
                .withInstanceCoreParam( CausalClusteringSettings.server_groups, id -> serverGroups.get( id ) );

        // given
        Cluster cluster = clusterRule.startCluster();
        int numberOfNodesToCreate = 100;

        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.success();
        } );

        // 0, 1, 2 are core instances
        createLabelledNodesWithProperty( cluster, numberOfNodesToCreate, label( "Foo" ),
                () -> Pair.of( "foobar", String.format( "baz_bat%s", UUID.randomUUID() ) ) );

        // 3, 4 are other DCs
        ReadReplica east3 = cluster.addReadReplicaWithId( 3 );
        east3.start();
        ReadReplica west4 = cluster.addReadReplicaWithId( 4 );
        west4.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            coreClusterMember.disableCatchupServer();
        }

        // 5, 6 are other DCs
        ReadReplica east5 = cluster.addReadReplicaWithId( 5 );
        east5.setUpstreamDatabaseSelectionStrategy( "connect-randomly-within-server-group" );
        east5.start();
        ReadReplica west6 = cluster.addReadReplicaWithId( 6 );
        west6.setUpstreamDatabaseSelectionStrategy( "connect-randomly-within-server-group" );
        west6.start();

        checkDataHasReplicatedToReadReplicas( cluster, numberOfNodesToCreate );

    }
}
