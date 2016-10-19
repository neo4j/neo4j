/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

import static org.neo4j.helpers.SocketAddressFormat.socketAddress;

class HazelcastClusterTopology
{
    static final String READ_REPLICA_BOLT_ADDRESS_MAP_NAME = "read-replicas"; // hz client uuid string -> boltAddress
    // string
    static final String CLUSTER_UUID = "cluster_uuid";
    static final String MEMBER_UUID = "member_uuid";
    static final String TRANSACTION_SERVER = "transaction_server";
    static final String DISCOVERY_SERVER = "discovery_server";
    static final String RAFT_SERVER = "raft_server";
    static final String CLIENT_CONNECTOR_ADDRESSES = "client_connector_addresses";

    static ReadReplicaTopology getReadReplicaTopology( HazelcastInstance hazelcastInstance, Log log )
    {
        Set<ReadReplicaAddresses> readReplicas = emptySet();
        ClusterId clusterId = null;

        if ( hazelcastInstance != null )
        {
            readReplicas = readReplicas( hazelcastInstance );
            clusterId = getClusterId( hazelcastInstance );
        }
        else
        {
            log.info( "Cannot currently bind to distributed discovery service." );
        }

        return new ReadReplicaTopology( clusterId, readReplicas );
    }

    static CoreTopology getCoreTopology( HazelcastInstance hazelcastInstance, Log log )
    {
        Map<MemberId,CoreAddresses> coreMembers = emptyMap();
        boolean canBeBootstrapped = false;
        ClusterId clusterId = null;

        if ( hazelcastInstance != null )
        {
            Set<Member> hzMembers = hazelcastInstance.getCluster().getMembers();
            canBeBootstrapped = canBeBootstrapped( hzMembers );

            coreMembers = toCoreMemberMap( hzMembers, log );

            clusterId = getClusterId( hazelcastInstance );
        }
        else
        {
            log.info( "Cannot currently bind to distributed discovery service." );
        }

        return new CoreTopology( clusterId, canBeBootstrapped, coreMembers );
    }

    private static ClusterId getClusterId( HazelcastInstance hazelcastInstance )
    {
        IAtomicReference<UUID> uuidReference = hazelcastInstance.getAtomicReference( CLUSTER_UUID );
        UUID uuid = uuidReference.get();
        return uuid != null ? new ClusterId( uuid ) : null;
    }

    static boolean casClusterId( HazelcastInstance hazelcastInstance, ClusterId clusterId )
    {
        IAtomicReference<UUID> uuidReference = hazelcastInstance.getAtomicReference( CLUSTER_UUID );
        return uuidReference.compareAndSet( null, clusterId.uuid() ) || uuidReference.get().equals( clusterId.uuid() );
    }

    private static Set<ReadReplicaAddresses> readReplicas( HazelcastInstance hazelcastInstance )
    {
        IMap<String/*uuid*/, String/*boltAddress*/> readReplicaMap = hazelcastInstance.getMap(
                READ_REPLICA_BOLT_ADDRESS_MAP_NAME );

        return readReplicaMap
                .entrySet().stream()
                .map( entry -> new ReadReplicaAddresses( ClientConnectorAddresses.fromString( entry.getValue() ) ) )
                .collect( toSet() );
    }

    private static boolean canBeBootstrapped( Set<Member> coreMembers )
    {
        Iterator<Member> iterator = coreMembers.iterator();
        return iterator.hasNext() && iterator.next().localMember();
    }

    static Map<MemberId,CoreAddresses> toCoreMemberMap( Set<Member> members, Log log )
    {
        Map<MemberId,CoreAddresses> coreMembers = new HashMap<>();

        for ( Member member : members )
        {
            try
            {
                Pair<MemberId,CoreAddresses> pair = extractMemberAttributes( member );
                coreMembers.put( pair.first(), pair.other() );
            }
            catch ( IllegalArgumentException e )
            {
                log.warn( "Incomplete member attributes supplied from Hazelcast", e );
            }
        }

        return coreMembers;
    }

    static MemberAttributeConfig buildMemberAttributes( MemberId myself, Config config )
    {
        MemberAttributeConfig memberAttributeConfig = new MemberAttributeConfig();
        memberAttributeConfig.setStringAttribute( MEMBER_UUID, myself.getUuid().toString() );

        AdvertisedSocketAddress discoveryAddress =
                config.get( CausalClusteringSettings.discovery_advertised_address );
        memberAttributeConfig.setStringAttribute( DISCOVERY_SERVER, discoveryAddress.toString() );

        AdvertisedSocketAddress transactionSource =
                config.get( CausalClusteringSettings.transaction_advertised_address );
        memberAttributeConfig.setStringAttribute( TRANSACTION_SERVER, transactionSource.toString() );

        AdvertisedSocketAddress raftAddress = config.get( CausalClusteringSettings.raft_advertised_address );
        memberAttributeConfig.setStringAttribute( RAFT_SERVER, raftAddress.toString() );

        ClientConnectorAddresses clientConnectorAddresses = ClientConnectorAddresses.extractFromConfig( config );
        memberAttributeConfig.setStringAttribute( CLIENT_CONNECTOR_ADDRESSES, clientConnectorAddresses.toString() );
        return memberAttributeConfig;
    }

    static Pair<MemberId,CoreAddresses> extractMemberAttributes( Member member )
    {
        MemberId memberId = new MemberId( UUID.fromString( member.getStringAttribute( MEMBER_UUID ) ) );

        return Pair.of( memberId, new CoreAddresses(
                socketAddress( member.getStringAttribute( RAFT_SERVER ), AdvertisedSocketAddress::new ),
                socketAddress( member.getStringAttribute( TRANSACTION_SERVER ), AdvertisedSocketAddress::new ),
                ClientConnectorAddresses.fromString( member.getStringAttribute( CLIENT_CONNECTOR_ADDRESSES ) )
        ) );
    }
}
