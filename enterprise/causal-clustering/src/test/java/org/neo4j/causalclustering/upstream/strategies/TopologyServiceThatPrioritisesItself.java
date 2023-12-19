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
package org.neo4j.causalclustering.upstream.strategies;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

class TopologyServiceThatPrioritisesItself implements TopologyService
{
    private final MemberId myself;
    private final String matchingGroupName;

    MemberId coreNotSelf = new MemberId( new UUID( 321, 654 ) );
    MemberId readReplicaNotSelf = new MemberId( new UUID( 432, 543 ) );

    TopologyServiceThatPrioritisesItself( MemberId myself, String matchingGroupName )
    {
        this.myself = myself;
        this.matchingGroupName = matchingGroupName;
    }

    @Override
    public String localDBName()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public CoreTopology allCoreServers()
    {
        boolean canBeBootstrapped = true;
        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( myself, coreServerInfo() );
        coreMembers.put( coreNotSelf, coreServerInfo() );
        return new CoreTopology( new ClusterId( new UUID( 99, 88 ) ), canBeBootstrapped, coreMembers );
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return allCoreServers();
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        Map<MemberId,ReadReplicaInfo> readReplicaMembers = new HashMap<>();
        readReplicaMembers.put( myself, readReplicaInfo( matchingGroupName ) );
        readReplicaMembers.put( readReplicaNotSelf, readReplicaInfo( matchingGroupName ) );
        return new ReadReplicaTopology( readReplicaMembers );
    }

    @Override
    public ReadReplicaTopology localReadReplicas()
    {
        return allReadReplicas();
    }

    @Override
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    private static CoreServerInfo coreServerInfo( String... groupNames )
    {
        AdvertisedSocketAddress anyRaftAddress = new AdvertisedSocketAddress( "hostname", 1234 );
        AdvertisedSocketAddress anyCatchupServer = new AdvertisedSocketAddress( "hostname", 5678 );
        ClientConnectorAddresses clientConnectorAddress = new ClientConnectorAddresses( Collections.emptyList() );
        Set<String> groups = new HashSet<>( Arrays.asList( groupNames ) );
        return new CoreServerInfo( anyRaftAddress, anyCatchupServer, clientConnectorAddress, groups, "dbName" );
    }

    private static ReadReplicaInfo readReplicaInfo( String... groupNames )
    {
        ClientConnectorAddresses clientConnectorAddresses = new ClientConnectorAddresses( Collections.emptyList() );
        AdvertisedSocketAddress catchupServerAddress = new AdvertisedSocketAddress( "hostname", 2468 );
        Set<String> groups = new HashSet<>( Arrays.asList( groupNames ) );
        ReadReplicaInfo readReplicaInfo = new ReadReplicaInfo( clientConnectorAddresses, catchupServerAddress, groups, "dbName" );
        return readReplicaInfo;
    }
}
