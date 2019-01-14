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
package org.neo4j.causalclustering.discovery;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
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
    private final List<SharedDiscoveryCoreClient> listeningClients;
    private final ConcurrentMap<String,ClusterId> clusterIdDbNames;
    private final ConcurrentMap<String,LeaderInfo> leaderMap;
    private final CountDownLatch enoughMembers;

    SharedDiscoveryService()
    {
        coreMembers = new ConcurrentHashMap<>();
        readReplicas = new ConcurrentHashMap<>();
        listeningClients = new CopyOnWriteArrayList<>();
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
