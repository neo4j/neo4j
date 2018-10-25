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

import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.CollectorsUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.stream.Streams;

import static java.util.Collections.emptyMap;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.refuse_to_be_leader;
import static org.neo4j.helpers.SocketAddressParser.socketAddress;
import static org.neo4j.helpers.collection.Iterables.asSet;

public final class HazelcastClusterTopology
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
    static final String CLUSTER_UUID_DB_NAME_MAP = "cluster_uuid";
    static final String SERVER_GROUPS_MULTIMAP = "groups";
    static final String READ_REPLICA_TRANSACTION_SERVER_ADDRESS_MAP = "read-replica-transaction-servers";
    static final String READ_REPLICA_BOLT_ADDRESS_MAP = "read_replicas"; // hz client uuid string -> boltAddress string
    static final String READ_REPLICA_MEMBER_ID_MAP = "read-replica-member-ids";
    static final String READ_REPLICAS_DB_NAME_MAP = "read_replicas_database_names";
    static final String DB_NAME_LEADER_TERM_PREFIX = "leader_term_for_database_name_";

    // the attributes used for reconstructing read replica information
    static final Set<String> RR_ATTR_KEYS = Stream.of( READ_REPLICA_BOLT_ADDRESS_MAP, READ_REPLICA_TRANSACTION_SERVER_ADDRESS_MAP,
            READ_REPLICA_MEMBER_ID_MAP, READ_REPLICAS_DB_NAME_MAP ).collect( Collectors.toSet() );

    // the attributes used for reconstructing core member information
    static final Set<String> CORE_ATTR_KEYS = Stream.of( MEMBER_UUID, RAFT_SERVER, TRANSACTION_SERVER, CLIENT_CONNECTOR_ADDRESSES,
            MEMBER_DB_NAME ).collect( Collectors.toSet() );

    private HazelcastClusterTopology()
    {
    }

    static ReadReplicaTopology getReadReplicaTopology( HazelcastInstance hazelcastInstance, Log log )
    {
        Map<MemberId,ReadReplicaInfo> readReplicas = emptyMap();

        if ( hazelcastInstance != null )
        {
            readReplicas = readReplicas( hazelcastInstance, log );
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
        IMap<String, UUID> uuidPerDbCluster = hazelcastInstance.getMap( CLUSTER_UUID_DB_NAME_MAP );
        UUID uuid = uuidPerDbCluster.get( dbName );
        return uuid != null ? new ClusterId( uuid ) : null;
    }

    private static Set<String> getDBNames( HazelcastInstance hazelcastInstance )
    {
        IMap<String, UUID> uuidPerDbCluster = hazelcastInstance.getMap( CLUSTER_UUID_DB_NAME_MAP );
        return uuidPerDbCluster.keySet();
    }

    public static Map<MemberId,RoleInfo> getCoreRoles( HazelcastInstance hazelcastInstance, Set<MemberId> coreMembers )
    {

        Set<String> dbNames = getDBNames( hazelcastInstance );
        Set<MemberId> allLeaders = dbNames.stream()
                .map( n -> getLeaderForDBName( hazelcastInstance, n ) )
                .filter( Optional::isPresent )
                .map( l -> l.get().memberId() )
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

    static Map<MemberId,ReadReplicaInfo> readReplicas( HazelcastInstance hazelcastInstance, Log log )
    {
        Pair<Set<String>,Map<String,IMap<String,String>>> validatedSimpleAttrMaps = validatedSimpleAttrMaps( hazelcastInstance );
        Set<String> missingAttrKeys = validatedSimpleAttrMaps.first();
        Map<String,IMap<String,String>> simpleAttrMaps = validatedSimpleAttrMaps.other();

        MultiMap<String,String> serverGroupsMap = hazelcastInstance.getMultiMap( SERVER_GROUPS_MULTIMAP );

        if ( serverGroupsMap == null )
        {
            missingAttrKeys.add( SERVER_GROUPS_MULTIMAP );
        }

        if ( !missingAttrKeys.isEmpty() )
        {
            // We might well not have any read replicas, in which case missing maps is not an error, but we *can't* have some maps and not others
            boolean missingAllKeys = missingAttrKeys.containsAll( RR_ATTR_KEYS ) && missingAttrKeys.contains( SERVER_GROUPS_MULTIMAP );
            if ( !missingAllKeys )
            {
                String missingAttrs = String.join( ", ", missingAttrKeys );
                log.warn( "Some, but not all, of the read replica attribute maps are null, including %s", missingAttrs );
            }

            return emptyMap();
        }

        Stream<String> readReplicaHzIds = simpleAttrMaps.get( READ_REPLICA_BOLT_ADDRESS_MAP ).keySet().stream();

        Map<MemberId,ReadReplicaInfo> validatedReadReplicas = readReplicaHzIds
                .flatMap( hzId -> Streams.ofNullable( buildReadReplicaFromAttrMap( hzId, simpleAttrMaps, serverGroupsMap, log ) ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        return validatedReadReplicas;
    }

    /**
     * Retrieves the various maps containing attributes about read replicas from hazelcast. If any maps do not exist, keep track of their keys for logging.
     */
    private static Pair<Set<String>,Map<String,IMap<String,String>>> validatedSimpleAttrMaps( HazelcastInstance hazelcastInstance )
    {
        Set<String> missingAttrKeys = new HashSet<>();
        Map<String,IMap<String,String>> validatedSimpleAttrMaps = new HashMap<>();

        for ( String attrMapKey : RR_ATTR_KEYS )
        {
            IMap<String,String> attrMap = hazelcastInstance.getMap( attrMapKey );
            if ( attrMap == null )
            {
                missingAttrKeys.add( attrMapKey );
            }
            else
            {
                validatedSimpleAttrMaps.put( attrMapKey, attrMap );
            }
        }

        return Pair.of( missingAttrKeys, validatedSimpleAttrMaps );
    }

    /**
     * Given a hazelcast member id and a set of non-null attribute maps, this method builds a discovery representation of a read replica
     * (i.e. `Pair<MemberId,ReadReplicaInfo>`). Any missing attributes which are missing for a given hazelcast member id are logged and this
     * method will return null.
     */
    private static Pair<MemberId,ReadReplicaInfo> buildReadReplicaFromAttrMap( String hzId, Map<String,IMap<String,String>> simpleAttrMaps,
            MultiMap<String,String> serverGroupsMap, Log log )
    {
        Map<String,String> memberAttrs = simpleAttrMaps.entrySet().stream()
                .map( e -> Pair.of( e.getKey(), e.getValue().get( hzId ) ) )
                .filter( p -> hasAttribute( p, hzId, log ) )
                .collect( CollectorsUtil.pairsToMap() );

        if ( !memberAttrs.keySet().containsAll( RR_ATTR_KEYS ) )
        {
            return null;
        }

        Collection<String> memberServerGroups = serverGroupsMap.get( hzId );
        if ( memberServerGroups == null )
        {
            log.warn( "Missing attribute %s for read replica with hz id %s", SERVER_GROUPS_MULTIMAP, hzId );
            return null;
        }

        ClientConnectorAddresses boltAddresses = ClientConnectorAddresses.fromString( memberAttrs.get( READ_REPLICA_BOLT_ADDRESS_MAP ) );
        AdvertisedSocketAddress catchupAddress = socketAddress( memberAttrs.get( READ_REPLICA_TRANSACTION_SERVER_ADDRESS_MAP ), AdvertisedSocketAddress::new );
        MemberId memberId = new MemberId( UUID.fromString( memberAttrs.get( READ_REPLICA_MEMBER_ID_MAP ) ) );
        String memberDbName = memberAttrs.get( READ_REPLICAS_DB_NAME_MAP );
        Set<String> serverGroupSet = asSet( memberServerGroups );

        ReadReplicaInfo rrInfo = new ReadReplicaInfo( boltAddresses, catchupAddress, serverGroupSet, memberDbName );
        return Pair.of( memberId, rrInfo );
    }

    private static boolean hasAttribute( Pair<String,String> memberAttr, String hzId, Log log )
    {
        if ( memberAttr.other() == null )
        {
            log.warn( "Missing attribute %s for read replica with hz id %s", memberAttr.first(), hzId  );
            return false;
        }
        return true;
    }

    static void casLeaders( HazelcastInstance hazelcastInstance, LeaderInfo leaderInfo, String dbName, Log log )
    {
        IAtomicReference<LeaderInfo> leaderRef = hazelcastInstance.getAtomicReference( DB_NAME_LEADER_TERM_PREFIX + dbName );

        LeaderInfo current = leaderRef.get();
        Optional<LeaderInfo> currentOpt = Optional.ofNullable( current );

        boolean sameLeader =  currentOpt.map( LeaderInfo::memberId ).equals( Optional.ofNullable( leaderInfo.memberId() ) );

        int termComparison =  currentOpt.map( l -> Long.compare( l.term(), leaderInfo.term() ) ).orElse( -1 );

        boolean greaterTermExists = termComparison > 0;

        boolean sameTermButNoStepdown = termComparison == 0 && !leaderInfo.isSteppingDown();

        if ( sameLeader || greaterTermExists || sameTermButNoStepdown )
        {
            return;
        }

        boolean success = leaderRef.compareAndSet( current, leaderInfo );
        if ( !success )
        {
            log.warn( "Fail to set new leader info: %s. Latest leader info: %s.", leaderInfo, leaderRef.get() );
        }
    }

    private static Optional<LeaderInfo> getLeaderForDBName( HazelcastInstance hazelcastInstance, String dbName )
    {
        IAtomicReference<LeaderInfo> leader = hazelcastInstance.getAtomicReference( DB_NAME_LEADER_TERM_PREFIX + dbName );
        return Optional.ofNullable( leader.get() );
    }

    private static boolean canBeBootstrapped( HazelcastInstance hazelcastInstance, Config config )
    {
        Set<Member> members = hazelcastInstance.getCluster().getMembers();
        String dbName = config.get( CausalClusteringSettings.database );

        Predicate<Member> acceptsToBeLeader = m -> !m.getBooleanAttribute( REFUSE_TO_BE_LEADER_KEY );
        Predicate<Member> hostsMyDb = m -> dbName.equals( m.getStringAttribute( MEMBER_DB_NAME ) );

        Stream<Member> membersWhoCanLeadForMyDb = members.stream().filter( acceptsToBeLeader ).filter( hostsMyDb );

        Optional<Member> firstAppropriateMember = membersWhoCanLeadForMyDb.findFirst();

        return firstAppropriateMember.map( Member::localMember ).orElse( false );
    }

    static Map<MemberId,CoreServerInfo> toCoreMemberMap( Set<Member> members, Log log,
            HazelcastInstance hazelcastInstance )
    {
        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        MultiMap<String,String> serverGroupsMMap = hazelcastInstance.getMultiMap( SERVER_GROUPS_MULTIMAP );

        for ( Member member : members )
        {
            Map<String,String> attrMap = new HashMap<>();
            boolean incomplete = false;
            for ( String attrKey : CORE_ATTR_KEYS )
            {
                String attrValue = member.getStringAttribute( attrKey );
                if ( attrValue == null )
                {
                    log.warn( "Missing member attribute '%s' for member %s", attrKey, member );
                    incomplete = true;
                }
                else
                {
                    attrMap.put( attrKey, attrValue );
                }
            }

            if ( incomplete )
            {
                continue;
            }

            CoreServerInfo coreServerInfo = new CoreServerInfo(
                    socketAddress( attrMap.get( RAFT_SERVER ), AdvertisedSocketAddress::new ),
                    socketAddress( attrMap.get( TRANSACTION_SERVER ), AdvertisedSocketAddress::new ),
                    ClientConnectorAddresses.fromString( attrMap.get( CLIENT_CONNECTOR_ADDRESSES ) ),
                    asSet( serverGroupsMMap.get( attrMap.get( MEMBER_UUID ) ) ),
                    attrMap.get( MEMBER_DB_NAME ),
                    member.getBooleanAttribute( REFUSE_TO_BE_LEADER_KEY ) );

            MemberId memberId = new MemberId( UUID.fromString( attrMap.get( MEMBER_UUID ) ) );
            coreMembers.put( memberId, coreServerInfo );
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
