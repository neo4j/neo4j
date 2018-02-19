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

import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.refuse_to_be_leader;
import static java.util.stream.Stream.concat;
import static java.util.stream.Stream.of;
import static org.neo4j.helpers.SocketAddressParser.socketAddress;
import static org.neo4j.helpers.collection.Iterables.asSet;

public class HazelcastClusterTopology
{
    // per server attributes
    private static final String DISCOVERY_SERVER = "discovery_server"; // not currently used
    static final String MEMBER_UUID = "member_uuid";
    static final String TRANSACTION_SERVER = "transaction_server";
    static final String RAFT_SERVER = "raft_server";
    static final String CLIENT_CONNECTOR_ADDRESSES = "client_connector_addresses";
    static final String MEMBER_DB_NAME = "member_database_name";

    private static final String REFUSE_TO_BE_LEADER_KEY = "refuseToBeLeader";

    // cluster-wide attributes
    private static final String CLUSTER_UUID_DB_NAME_MAP = "cluster_uuid";
    private static final String SERVER_GROUPS_MULTIMAP = "groups";
    static final String READ_REPLICA_TRANSACTION_SERVER_ADDRESS_MAP = "read-replica-transaction-servers";
    static final String READ_REPLICA_BOLT_ADDRESS_MAP = "read_replicas"; // hz client uuid string -> boltAddress string
    static final String READ_REPLICA_MEMBER_ID_MAP = "read-replica-member-ids";
    static final String READ_REPLICAS_DB_NAME_MAP = "read_replicas_database_names";
    private static final String DB_NAME_LEADER_TERM_PREFIX = "leader_term_for_database_name_";

    private HazelcastClusterTopology()
    {
    }

    static ReadReplicaTopology getReadReplicaTopology( HazelcastInstance hazelcastInstance, Log log )
    {
        Map<MemberId,ReadReplicaInfo> readReplicas = emptyMap();

        if ( hazelcastInstance != null )
        {
            readReplicas = readReplicas( hazelcastInstance );
        }
        else
        {
            log.info( "Cannot currently bind to distributed discovery service." );
        }

        return new ReadReplicaTopology( readReplicas );
    }

    static CoreTopology getCoreTopology( HazelcastInstance hazelcastInstance, Config config, Log log )
    {
        Map<MemberId,CoreServerInfo> coreMembers = emptyMap();
        boolean canBeBootstrapped = false;
        ClusterId clusterId = null;
        String dbName = config.get( CausalClusteringSettings.database );

        if ( hazelcastInstance != null )
        {
            Set<Member> hzMembers = hazelcastInstance.getCluster().getMembers();
            canBeBootstrapped = canBeBootstrapped( hazelcastInstance, config );

            coreMembers = toCoreMemberMap( hzMembers, log, hazelcastInstance );

            clusterId = getClusterId( hazelcastInstance, dbName );
        }
        else
        {
            log.info( "Cannot currently bind to distributed discovery service." );
        }

        return new CoreTopology( clusterId, canBeBootstrapped, coreMembers );
    }

    public static Map<MemberId,AdvertisedSocketAddress> extractCatchupAddressesMap( CoreTopology coreTopology,
            ReadReplicaTopology rrTopology )
    {
        Map<MemberId,AdvertisedSocketAddress> catchupAddressMap = new HashMap<>();

        for ( Map.Entry<MemberId,CoreServerInfo> entry : coreTopology.members().entrySet() )
        {
            catchupAddressMap.put( entry.getKey(), entry.getValue().getCatchupServer() );
        }

        for ( Map.Entry<MemberId,ReadReplicaInfo> entry : rrTopology.members().entrySet() )
        {
            catchupAddressMap.put( entry.getKey(), entry.getValue().getCatchupServer() );

        }

        return catchupAddressMap;
    }

    private static ClusterId getClusterId( HazelcastInstance hazelcastInstance, String dbName )
    {
        //TODO: Update to Optionals
        IMap<String, UUID> uuidPerDbCluster = hazelcastInstance.getMap( CLUSTER_UUID_DB_NAME_MAP );
        UUID uuid = uuidPerDbCluster.get( dbName );
        return uuid != null ? new ClusterId( uuid ) : null;
    }

    public static Set<String> getDBNames( HazelcastInstance hazelcastInstance )
    {
        IMap<String, UUID> uuidPerDbCluster = hazelcastInstance.getMap( CLUSTER_UUID_DB_NAME_MAP );
        return uuidPerDbCluster.keySet();
    }

    public static Map<MemberId,RoleInfo> getCoreRoles( HazelcastInstance hazelcastInstance, Set<MemberId> coreMembers )
    {

        Set<String> dbNames = getDBNames( hazelcastInstance );
        Set<MemberId> allLeaders = dbNames.stream()
                .map( n -> getLeaderForDBName( hazelcastInstance, n ).memberId() )
                .collect( Collectors.toSet() );

        Function<MemberId,RoleInfo> roleMapper = m -> allLeaders.contains( m ) ? RoleInfo.LEADER : RoleInfo.FOLLOWER;

        return coreMembers.stream().collect( Collectors.toMap( Function.identity(), roleMapper ) );
    }

    static boolean casClusterId( HazelcastInstance hazelcastInstance, ClusterId clusterId, String dbName )
    {
        IMap<String, UUID> uuidPerDbCluster = hazelcastInstance.getMap( CLUSTER_UUID_DB_NAME_MAP );
        UUID uuid = uuidPerDbCluster.putIfAbsent( dbName, clusterId.uuid() );
        return uuid == null || clusterId.uuid().equals( uuid );
    }

    private static Map<MemberId,ReadReplicaInfo> readReplicas( HazelcastInstance hazelcastInstance )
    {
        Map<MemberId,ReadReplicaInfo> result = new HashMap<>();

        IMap<String/*uuid*/,String/*boltAddress*/> clientAddressMap =
                hazelcastInstance.getMap( READ_REPLICA_BOLT_ADDRESS_MAP );
        IMap<String,String> txServerMap = hazelcastInstance.getMap( READ_REPLICA_TRANSACTION_SERVER_ADDRESS_MAP );
        IMap<String,String> memberIdMap = hazelcastInstance.getMap( READ_REPLICA_MEMBER_ID_MAP );
        MultiMap<String,String> serverGroups = hazelcastInstance.getMultiMap( SERVER_GROUPS_MULTIMAP );
        IMap<String, String> memberDbMap = hazelcastInstance.getMap( READ_REPLICAS_DB_NAME_MAP );

        if ( of( clientAddressMap, txServerMap, memberIdMap, serverGroups ).anyMatch( Objects::isNull ) )
        {
            return result;
        }

        for ( String hzUUID : clientAddressMap.keySet() )
        {
            String sAddresses = clientAddressMap.get( hzUUID );
            String sCatchupAddress = txServerMap.get( hzUUID );
            String sMemberId = memberIdMap.get( hzUUID );
            String dbName = memberDbMap.get( hzUUID );
            Collection<String> sServerGroups = serverGroups.get( hzUUID );

            if ( concat( of( sServerGroups ), of( sAddresses, sCatchupAddress, sMemberId ) ).anyMatch( Objects::isNull ) )
            {
                continue;
            }

            ClientConnectorAddresses clientConnectorAddresses = ClientConnectorAddresses.fromString( sAddresses );
            AdvertisedSocketAddress catchupAddress = socketAddress( sCatchupAddress, AdvertisedSocketAddress::new );

            ReadReplicaInfo readReplicaInfo = new ReadReplicaInfo( clientConnectorAddresses, catchupAddress, asSet( sServerGroups ), dbName );
            result.put( new MemberId( UUID.fromString( sMemberId ) ), readReplicaInfo );
        }
        return result;
    }

    static boolean casLeaders( HazelcastInstance hazelcastInstance, MemberId leader, long term, String dbName )
    {
        IAtomicReference<RaftLeader> leaderRef = hazelcastInstance.getAtomicReference( DB_NAME_LEADER_TERM_PREFIX + dbName );

        RaftLeader expected = leaderRef.get();

        boolean noUpdate = Optional.ofNullable( expected ).map( RaftLeader::memberId ).equals( Optional.ofNullable( leader ) );

        boolean greaterOrEqualTermExists = Optional.ofNullable( expected ).map(l -> l.term() >= term ).orElse( false );

        if ( greaterOrEqualTermExists || noUpdate )
        {
            return false;
        }

        return leaderRef.compareAndSet( expected, new RaftLeader( leader, term ) );
    }

    static RaftLeader getLeaderForDBName( HazelcastInstance hazelcastInstance, String dbName )
    {
        IAtomicReference<RaftLeader> leader = hazelcastInstance.getAtomicReference( DB_NAME_LEADER_TERM_PREFIX + dbName );
        return leader.get();
    }

    private static boolean canBeBootstrapped( HazelcastInstance hazelcastInstance, Config config )
    {
        //TODO: Refactor for clarity
        Set<Member> members = hazelcastInstance.getCluster().getMembers();

        String dbName = config.get( CausalClusteringSettings.database );

        if ( config.get( refuse_to_be_leader ) )
        {
            return false;
        }
        else
        {
            for ( Member member : members )
            {
                boolean localDb = dbName.equals( member.getStringAttribute( MEMBER_DB_NAME ) );

                if ( !member.getBooleanAttribute( REFUSE_TO_BE_LEADER_KEY ) && localDb )
                {
                    return member.localMember();
                }
            }
            return false;
        }
    }

    static Map<MemberId,CoreServerInfo> toCoreMemberMap( Set<Member> members, Log log,
            HazelcastInstance hazelcastInstance )
    {
        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MultiMap<String,String> serverGroupsMMap = hazelcastInstance.getMultiMap( SERVER_GROUPS_MULTIMAP );

        for ( Member member : members )
        {
            try
            {
                MemberId memberId = new MemberId( UUID.fromString( member.getStringAttribute( MEMBER_UUID ) ) );
                String dbName = member.getStringAttribute( MEMBER_DB_NAME );

                CoreServerInfo coreServerInfo = new CoreServerInfo(
                        socketAddress( member.getStringAttribute( RAFT_SERVER ), AdvertisedSocketAddress::new ),
                        socketAddress( member.getStringAttribute( TRANSACTION_SERVER ), AdvertisedSocketAddress::new ),
                        ClientConnectorAddresses.fromString( member.getStringAttribute( CLIENT_CONNECTOR_ADDRESSES ) ),
                        asSet( serverGroupsMMap.get( memberId.getUuid().toString() ) ),
                        dbName );

                coreMembers.put( memberId, coreServerInfo );
            }
            catch ( IllegalArgumentException e )
            {
                log.warn( "Incomplete member attributes supplied from Hazelcast", e );
            }
        }

        return coreMembers;
    }

    static void refreshGroups( HazelcastInstance hazelcastInstance, String memberId, List<String> groups )
    {
        MultiMap<String,String> groupsMap = hazelcastInstance.getMultiMap( SERVER_GROUPS_MULTIMAP );
        Collection<String> existing = groupsMap.get( memberId );

        Set<String> superfluous = existing.stream().filter( t -> !groups.contains( t ) ).collect( Collectors.toSet() );
        Set<String> missing = groups.stream().filter( t -> !existing.contains( t ) ).collect( Collectors.toSet() );

        missing.forEach( group -> groupsMap.put( memberId, group ) );
        superfluous.forEach( group -> groupsMap.remove( memberId, group ) );
    }

    static MemberAttributeConfig buildMemberAttributesForCore( MemberId myself, Config config )
    {

        MemberAttributeConfig memberAttributeConfig = new MemberAttributeConfig();
        memberAttributeConfig.setStringAttribute( MEMBER_UUID, myself.getUuid().toString() );

        AdvertisedSocketAddress discoveryAddress = config.get( CausalClusteringSettings.discovery_advertised_address );
        memberAttributeConfig.setStringAttribute( DISCOVERY_SERVER, discoveryAddress.toString() );

        AdvertisedSocketAddress transactionSource =
                config.get( CausalClusteringSettings.transaction_advertised_address );
        memberAttributeConfig.setStringAttribute( TRANSACTION_SERVER, transactionSource.toString() );

        AdvertisedSocketAddress raftAddress = config.get( CausalClusteringSettings.raft_advertised_address );
        memberAttributeConfig.setStringAttribute( RAFT_SERVER, raftAddress.toString() );

        ClientConnectorAddresses clientConnectorAddresses = ClientConnectorAddresses.extractFromConfig( config );
        memberAttributeConfig.setStringAttribute( CLIENT_CONNECTOR_ADDRESSES, clientConnectorAddresses.toString() );

        memberAttributeConfig.setBooleanAttribute( REFUSE_TO_BE_LEADER_KEY,
                config.get( refuse_to_be_leader )  );

        memberAttributeConfig.setStringAttribute( MEMBER_DB_NAME, config.get( CausalClusteringSettings.database ) );

        return memberAttributeConfig;
    }
}
