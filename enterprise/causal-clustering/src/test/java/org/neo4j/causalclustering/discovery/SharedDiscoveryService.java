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

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CountDownLatch;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;

public final class SharedDiscoveryService
{
    private static final int MIN_DISCOVERY_MEMBERS = 2;

    private final ConcurrentMap<MemberId,CoreServerInfo> coreMembers;
    private final ConcurrentMap<MemberId,ReadReplicaInfo> readReplicas;
    private final Set<SharedDiscoveryCoreClient> listeningClients;
    private final ConcurrentMap<String,ClusterId> clusterIdDbNames;
    private final ConcurrentMap<String,LeaderInfo> leaderMap;
    private final CountDownLatch enoughMembers;

    SharedDiscoveryService()
    {
        coreMembers = new ConcurrentHashMap<>();
        readReplicas = new ConcurrentHashMap<>();
        listeningClients = new ConcurrentSkipListSet<>();
        clusterIdDbNames = new ConcurrentHashMap<>();
        leaderMap = new ConcurrentHashMap<>();
        enoughMembers = new CountDownLatch( MIN_DISCOVERY_MEMBERS );
    }

    void waitForClusterFormation() throws InterruptedException
    {
        enoughMembers.await();
    }

    private boolean canBeBootstrapped( SharedDiscoveryCoreClient client )
    {
        Stream<SharedDiscoveryCoreClient> clientsWhoCanLeadForMyDb = listeningClients.stream()
                .filter( c -> !c.refusesToBeLeader() && c.localDBName().equals( client.localDBName() ) );

        Optional<SharedDiscoveryCoreClient> firstAppropriateClient = clientsWhoCanLeadForMyDb.findFirst();

        return firstAppropriateClient.map( c -> c.equals( client ) ).orElse( false );
    }

    CoreTopology getCoreTopology( SharedDiscoveryCoreClient client )
    {
        //Extract config from client
        String dbName = client.localDBName();
        boolean canBeBootstrapped = canBeBootstrapped( client );
        return getCoreTopology( dbName, canBeBootstrapped );
    }

    CoreTopology getCoreTopology( String dbName, boolean canBeBootstrapped  )
    {
        return new CoreTopology( clusterIdDbNames.get( dbName ),
                canBeBootstrapped, Collections.unmodifiableMap( coreMembers )  );
    }

    ReadReplicaTopology getReadReplicaTopology()
    {
        return new ReadReplicaTopology( Collections.unmodifiableMap( readReplicas ) );
    }

    void registerCoreMember( SharedDiscoveryCoreClient client )
    {
        CoreServerInfo previousMember = coreMembers.putIfAbsent( client.getMemberId(), client.getCoreServerInfo() );
        if ( previousMember == null )
        {
            listeningClients.add( client );
            enoughMembers.countDown();
            notifyCoreClients();
        }
    }

    void registerReadReplica( SharedDiscoveryReadReplicaClient client )
    {
        ReadReplicaInfo previousRR = readReplicas.putIfAbsent( client.getMemberId(), client.getReadReplicainfo() );
        if ( previousRR == null )
        {
            notifyCoreClients();
        }
    }

    void unRegisterCoreMember( SharedDiscoveryCoreClient client )
    {
        synchronized ( this )
        {
            listeningClients.remove( client );
            coreMembers.remove( client.getMemberId() );
        }
        notifyCoreClients();
    }

    void unRegisterReadReplica( SharedDiscoveryReadReplicaClient client )
    {
        readReplicas.remove( client.getMemberId() );
        notifyCoreClients();
    }

    void casLeaders( LeaderInfo leaderInfo, String dbName )
    {
        synchronized ( leaderMap )
        {
            Optional<LeaderInfo> current = Optional.ofNullable( leaderMap.get( dbName ) );

            boolean sameLeader = current.map( LeaderInfo::memberId ).equals( Optional.ofNullable( leaderInfo.memberId() ) );

            int termComparison = current.map( l -> Long.compare( l.term(), leaderInfo.term() ) ).orElse( -1 );

            boolean greaterTermExists = termComparison > 0;

            boolean sameTermButNoStepDown = termComparison == 0 && !leaderInfo.isSteppingDown();

            if ( !( greaterTermExists || sameTermButNoStepDown || sameLeader ) )
            {
                leaderMap.put( dbName, leaderInfo );
            }
        }
    }

    boolean casClusterId( ClusterId clusterId, String dbName )
    {
        ClusterId previousId = clusterIdDbNames.putIfAbsent( dbName, clusterId );

        boolean success = previousId == null || previousId.equals( clusterId );

        if ( success )
        {
            notifyCoreClients();
        }
        return success;
    }

    Map<MemberId,RoleInfo> getCoreRoles()
    {
        Set<String> dbNames = clusterIdDbNames.keySet();
        Set<MemberId> allLeaders = dbNames.stream()
                .map( dbName -> Optional.ofNullable( leaderMap.get( dbName ) ) )
                .filter( Optional::isPresent )
                .map( Optional::get )
                .map( LeaderInfo::memberId )
                .collect( Collectors.toSet());

        Function<MemberId,RoleInfo> roleMapper = m -> allLeaders.contains( m ) ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
        return coreMembers.keySet().stream().collect( Collectors.toMap( Function.identity(), roleMapper ) );
    }

    private synchronized void notifyCoreClients()
    {
        listeningClients.forEach( c -> {
            c.onCoreTopologyChange( getCoreTopology( c ) );
            c.onReadReplicaTopologyChange( getReadReplicaTopology() );
        } );
    }
}
