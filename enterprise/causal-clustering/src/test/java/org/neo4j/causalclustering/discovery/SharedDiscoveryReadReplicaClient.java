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
package org.neo4j.causalclustering.discovery;

import java.util.Map;
import java.util.Optional;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.helpers.SocketAddressParser.socketAddress;

class SharedDiscoveryReadReplicaClient implements TopologyService, Lifecycle
{
    private final SharedDiscoveryService sharedDiscoveryService;
    private final ReadReplicaInfo addresses;
    private final MemberId memberId;
    private final Log log;
    private final String dbName;

    SharedDiscoveryReadReplicaClient( SharedDiscoveryService sharedDiscoveryService, Config config, MemberId memberId,
            LogProvider logProvider )
    {
        this.sharedDiscoveryService = sharedDiscoveryService;
        this.dbName = config.get( CausalClusteringSettings.database );
        this.addresses = new ReadReplicaInfo( ClientConnectorAddresses.extractFromConfig( config ),
                socketAddress( config.get( CausalClusteringSettings.transaction_advertised_address ).toString(),
                        AdvertisedSocketAddress::new ), dbName );
        this.memberId = memberId;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void init()
    {
        // nothing to do
    }

    @Override
    public void start()
    {
        sharedDiscoveryService.registerReadReplica( this );
        log.info( "Registered read replica member id: %s at %s", memberId, addresses );
    }

    @Override
    public void stop()
    {
        sharedDiscoveryService.unRegisterReadReplica( this );
    }

    @Override
    public void shutdown()
    {
        // nothing to do
    }

    @Override
    public CoreTopology allCoreServers()
    {
        return sharedDiscoveryService.getCoreTopology( dbName, false );
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return allCoreServers().filterTopologyByDb( dbName );
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        return sharedDiscoveryService.getReadReplicaTopology();
    }

    @Override
    public ReadReplicaTopology localReadReplicas()
    {
        return allReadReplicas().filterTopologyByDb( dbName );
    }

    @Override
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        return sharedDiscoveryService.getCoreTopology( dbName, false )
                .find( upstream )
                .map( info -> Optional.of( info.getCatchupServer() ) )
                .orElseGet( () -> sharedDiscoveryService.getReadReplicaTopology()
                        .find( upstream )
                        .map( ReadReplicaInfo::getCatchupServer ) );
    }

    @Override
    public String localDBName()
    {
        return dbName;
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        return sharedDiscoveryService.getCoreRoles();
    }

    public MemberId getMemberId()
    {
        return memberId;
    }

    public ReadReplicaInfo getReadReplicainfo()
    {
        return addresses;
    }
}
