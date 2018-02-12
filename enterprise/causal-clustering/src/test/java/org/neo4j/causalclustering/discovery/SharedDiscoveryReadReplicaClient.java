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
package org.neo4j.causalclustering.discovery;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.management.CausalClustering;

import static org.neo4j.helpers.SocketAddressParser.socketAddress;

class SharedDiscoveryReadReplicaClient extends LifecycleAdapter implements TopologyService
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final ReadReplicaInfo addresses;
    private final MemberId memberId;
    private final Log log;

    SharedDiscoveryReadReplicaClient( SharedDiscoveryService sharedDiscoveryService, Config config, MemberId memberId,
            LogProvider logProvider )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.addresses = new ReadReplicaInfo( ClientConnectorAddresses.extractFromConfig( config ),
                socketAddress( config.get( CausalClusteringSettings.transaction_advertised_address ).toString(),
                        AdvertisedSocketAddress::new ), config.get( CausalClusteringSettings.database ) );
        this.memberId = memberId;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start()
    {
        sharedDiscoveryService.registerReadReplica( memberId, addresses );
        log.info( "Registered read replica member id: %s at %s", memberId, addresses );
    }

    @Override
    public void stop()
    {
        sharedDiscoveryService.unRegisterReadReplica( memberId);
    }

    @Override
    public CoreTopology coreServers()
    {
        return sharedDiscoveryService.coreTopology( null );
    }

    @Override
    public CoreTopology coreServers( String database )
    {
        CoreTopology topology = sharedDiscoveryService.coreTopology( null );
        log.info( "Core topology is %s", topology );

        Map<MemberId,CoreServerInfo> filteredCores = filterToplogyByDb( topology, database );
        return new CoreTopology( topology.clusterId(), topology.canBeBootstrapped(), filteredCores );
    }

    @Override
    public ReadReplicaTopology readReplicas( String database )
    {
        ReadReplicaTopology topology = sharedDiscoveryService.readReplicaTopology();
        log.info( "Read replica topology is %s", topology );

        Map<MemberId,ReadReplicaInfo> filteredRRs = filterToplogyByDb( topology, database );
        return new ReadReplicaTopology( filteredRRs );
    }

    private <T extends DiscoveryServerInfo> Map<MemberId, T> filterToplogyByDb( Topology<T> t, String dbName )
    {
        return t.members().entrySet().stream().filter(e -> e.getValue().getDatabaseName().equals( dbName ) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
    }

    @Override
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        return sharedDiscoveryService.coreTopology( null )
                .find( upstream )
                .map( info -> Optional.of( info.getCatchupServer() ) )
                .orElseGet( () -> sharedDiscoveryService.readReplicaTopology()
                        .find( upstream )
                        .map( ReadReplicaInfo::getCatchupServer ) );
    }
}
