/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.discovery;

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;

import static java.util.Collections.singletonList;
import static org.neo4j.causalclustering.discovery.ClientConnectorAddresses.Scheme.bolt;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class TestTopology
{
    private TestTopology()
    {
    }

    private static ClientConnectorAddresses wrapAsClientConnectorAddresses( AdvertisedSocketAddress advertisedSocketAddress )
    {
        return new ClientConnectorAddresses( singletonList( new ClientConnectorAddresses.ConnectorUri( bolt, advertisedSocketAddress ) ) );
    }

    public static CoreServerInfo addressesForCore( int id, boolean refuseToBeLeader )
    {
        AdvertisedSocketAddress raftServerAddress = new AdvertisedSocketAddress( "localhost", 3000 + id );
        AdvertisedSocketAddress catchupServerAddress = new AdvertisedSocketAddress( "localhost", 4000 + id );
        AdvertisedSocketAddress boltServerAddress = new AdvertisedSocketAddress( "localhost", 5000 + id );
        return new CoreServerInfo( raftServerAddress, catchupServerAddress, wrapAsClientConnectorAddresses( boltServerAddress ),
                asSet( "core", "core" + id ), "default", refuseToBeLeader );
    }

    public static Config configFor( CoreServerInfo coreServerInfo )
    {
        return Config.builder()
                .withSetting( CausalClusteringSettings.raft_advertised_address, coreServerInfo.getRaftServer().toString() )
                .withSetting( CausalClusteringSettings.transaction_advertised_address, coreServerInfo.getCatchupServer().toString() )
                .withSetting( "dbms.connector.bolt.listen_address", coreServerInfo.connectors().boltAddress().toString() )
                .withSetting( "dbms.connector.bolt.enabled", String.valueOf( true ) )
                .withSetting( CausalClusteringSettings.database, coreServerInfo.getDatabaseName() )
                .withSetting( CausalClusteringSettings.server_groups, String.join( ",", coreServerInfo.groups() ) )
                .withSetting( CausalClusteringSettings.refuse_to_be_leader, String.valueOf( coreServerInfo.refusesToBeLeader() ) )
                .build();
    }

    public static Config configFor( ReadReplicaInfo readReplicaInfo )
    {
        return Config.builder()
                .withSetting( "dbms.connector.bolt.listen_address", readReplicaInfo.connectors().boltAddress().toString() )
                .withSetting( "dbms.connector.bolt.enabled", String.valueOf( true ) )
                .withSetting( CausalClusteringSettings.transaction_advertised_address, readReplicaInfo.getCatchupServer().toString() )
                .withSetting( CausalClusteringSettings.server_groups, String.join( ",", readReplicaInfo.groups() ) )
                .withSetting( CausalClusteringSettings.database, readReplicaInfo.getDatabaseName() )
                .build();
    }

    public static ReadReplicaInfo addressesForReadReplica( int id )
    {
        AdvertisedSocketAddress clientConnectorSocketAddress = new AdvertisedSocketAddress( "localhost", 6000 + id );
        ClientConnectorAddresses clientConnectorAddresses = new ClientConnectorAddresses(
                singletonList( new ClientConnectorAddresses.ConnectorUri( bolt, clientConnectorSocketAddress ) ) );
        AdvertisedSocketAddress catchupSocketAddress = new AdvertisedSocketAddress( "localhost", 4000 + id );

        return new ReadReplicaInfo( clientConnectorAddresses, catchupSocketAddress,
                asSet( "replica", "replica" + id ), "default" );
    }

    public static Map<MemberId,ReadReplicaInfo> readReplicaInfoMap( int... ids )
    {
        return Arrays.stream( ids ).mapToObj( TestTopology::addressesForReadReplica ).collect( Collectors
                .toMap( p -> new MemberId( UUID.randomUUID() ), Function.identity() ) );
    }
}
