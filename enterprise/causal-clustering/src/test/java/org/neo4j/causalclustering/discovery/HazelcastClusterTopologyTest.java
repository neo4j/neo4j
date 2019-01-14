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

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.nio.Address;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.LeaderInfo;
import org.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.CLUSTER_UUID_DB_NAME_MAP;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.DB_NAME_LEADER_TERM_PREFIX;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.buildMemberAttributesForCore;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.toCoreMemberMap;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class HazelcastClusterTopologyTest
{
    private static final Set<String> GROUPS = asSet( "group1", "group2", "group3" );

    private static final Set<String> DB_NAMES = Stream.of( "foo", "bar", "baz" ).collect( Collectors.toSet() );
    private static final String DEFAULT_DB_NAME = "default";

    private static final IntFunction<HashMap<String, String>> DEFAULT_SETTINGS_GENERATOR = i -> {
        HashMap<String, String> settings = new HashMap<>();
        settings.put( CausalClusteringSettings.transaction_advertised_address.name(), "tx:" + (i + 1) );
        settings.put( CausalClusteringSettings.raft_advertised_address.name(), "raft:" + (i + 1) );
        settings.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
        settings.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
        settings.put( new BoltConnector( "bolt" ).advertised_address.name(), "bolt:" + (i + 1) );
        settings.put( new BoltConnector( "http" ).type.name(), "HTTP" );
        settings.put( new BoltConnector( "http" ).enabled.name(), "true" );
        settings.put( new BoltConnector( "http" ).advertised_address.name(), "http:" + (i + 1) );

        return settings;
    };

    private final HazelcastInstance hzInstance = mock( HazelcastInstance.class );

    @Before
    public void setup()
    {
        @SuppressWarnings( "unchecked" )
        MultiMap<String,String> serverGroupsMMap = mock( MultiMap.class );
        when( serverGroupsMMap.get( any() ) ).thenReturn( GROUPS );
        when( hzInstance.getMultiMap( anyString() ) ).thenReturn( (MultiMap) serverGroupsMMap );
    }

    private static List<Config> generateConfigs( int numConfigs )
    {
        return generateConfigs( numConfigs, DEFAULT_SETTINGS_GENERATOR );
    }

    private static List<Config> generateConfigs( int numConfigs, IntFunction<HashMap<String, String>> generator )
    {
        return IntStream.range(0, numConfigs).mapToObj( generator ).map( Config::defaults ).collect( Collectors.toList() );
    }

    @Test
    public void shouldCollectMembersAsAMap() throws Exception
    {
        // given
        int numMembers = 5;
        Set<Member> hazelcastMembers = new HashSet<>();
        List<MemberId> coreMembers = new ArrayList<>();

        List<Config> configs = generateConfigs( numMembers );

        for ( int i = 0; i < configs.size(); i++ )
        {
            MemberId mId = new MemberId( UUID.randomUUID() );
            coreMembers.add( mId );

            Config c = configs.get( i );
            Map<String, Object> attributes = buildMemberAttributesForCore( mId, c ).getAttributes();
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null, attributes, false ) );
        }

        // when
        Map<MemberId,CoreServerInfo> coreMemberMap = toCoreMemberMap( hazelcastMembers, NullLog.getInstance(), hzInstance );

        // then
        for ( int i = 0; i < numMembers; i++ )
        {
            CoreServerInfo coreServerInfo = coreMemberMap.get( coreMembers.get( i ) );
            assertEquals( new AdvertisedSocketAddress( "tx", i + 1 ), coreServerInfo.getCatchupServer() );
            assertEquals( new AdvertisedSocketAddress( "raft", i + 1 ), coreServerInfo.getRaftServer() );
            assertEquals( new AdvertisedSocketAddress( "bolt", i + 1 ), coreServerInfo.connectors().boltAddress() );
            assertEquals( coreServerInfo.getDatabaseName(), DEFAULT_DB_NAME );
            assertEquals( coreServerInfo.groups(), GROUPS );
        }
    }

    @Test
    public void shouldBuildMemberAttributedWithSpecifiedDBNames() throws Exception
    {
        //given
        int numMembers = 10;
        Set<Member> hazelcastMembers = new HashSet<>();
        List<MemberId> coreMembers = new ArrayList<>();

        Map<Integer, String> dbNames = CausalClusteringTestHelpers.distributeDatabaseNamesToHostNums( numMembers, DB_NAMES );
        IntFunction<HashMap<String, String>> generator = i -> {
            HashMap<String, String> settings =  DEFAULT_SETTINGS_GENERATOR.apply( i );
            settings.put( CausalClusteringSettings.database.name(), dbNames.get( i ) );
            return settings;
        };

        List<Config> configs = generateConfigs( numMembers, generator );

        for ( int i = 0; i < configs.size(); i++ )
        {
            MemberId mId = new MemberId( UUID.randomUUID() );
            coreMembers.add( mId );

            Config c = configs.get( i );
            Map<String, Object> attributes = buildMemberAttributesForCore( mId, c ).getAttributes();
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null, attributes, false ) );
        }

        // when
        Map<MemberId,CoreServerInfo> coreMemberMap = toCoreMemberMap( hazelcastMembers, NullLog.getInstance(), hzInstance );

        // then
        for ( int i = 0; i < numMembers; i++ )
        {
            CoreServerInfo coreServerInfo = coreMemberMap.get( coreMembers.get( i ) );
            String expectedDBName = dbNames.get( i );
            assertEquals( expectedDBName, coreServerInfo.getDatabaseName() );
        }

    }

    @Test
    public void shouldLogAndExcludeMembersWithMissingAttributes() throws Exception
    {
        // given
        int numMembers = 4;
        Set<Member> hazelcastMembers = new HashSet<>();
        List<MemberId> coreMembers = new ArrayList<>();

        IntFunction<HashMap<String, String>> generator = i -> {
            HashMap<String, String> settings =  DEFAULT_SETTINGS_GENERATOR.apply( i );
            settings.remove( CausalClusteringSettings.transaction_advertised_address.name() );
            settings.remove( CausalClusteringSettings.raft_advertised_address.name() );
            return settings;
        };

        List<Config> configs = generateConfigs( numMembers, generator );

        for ( int i = 0; i < configs.size(); i++ )
        {
            MemberId memberId = new MemberId( UUID.randomUUID() );
            coreMembers.add( memberId );
            Config c = configs.get( i );
            Map<String, Object> attributes = buildMemberAttributesForCore( memberId, c ).getAttributes();
            if ( i == 2 )
            {
                attributes.remove( HazelcastClusterTopology.RAFT_SERVER );
            }
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null, attributes, false ) );
        }

        // when
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Log log = logProvider.getLog( this.getClass() );
        Map<MemberId,CoreServerInfo> map = toCoreMemberMap( hazelcastMembers, log, hzInstance );

        // then
        assertThat( map.keySet(), hasItems( coreMembers.get( 0 ), coreMembers.get( 1 ), coreMembers.get( 3 ) ) );
        assertThat( map.keySet(), not( hasItems( coreMembers.get( 2 ) ) ) );
        logProvider.assertContainsMessageContaining( "Missing member attribute" );
    }

    @Test
    public void shouldCorrectlyReturnCoreMemberRoles()
    {
        //given
        int numMembers = 3;

        List<MemberId> members = IntStream.range(0, numMembers)
                .mapToObj( ignored -> new MemberId( UUID.randomUUID() ) ).collect( Collectors.toList() );

        @SuppressWarnings( "unchecked" )
        IAtomicReference<LeaderInfo> leaderRef = mock( IAtomicReference.class );
        MemberId chosenLeaderId = members.get( 0 );
        when( leaderRef.get() ).thenReturn( new LeaderInfo( chosenLeaderId, 0L ) );

        @SuppressWarnings( "unchecked" )
        IMap<String, UUID> uuidDBMap = mock( IMap.class );
        when( uuidDBMap.keySet() ).thenReturn( Collections.singleton( DEFAULT_DB_NAME ) );
        when( hzInstance.<LeaderInfo>getAtomicReference( startsWith( DB_NAME_LEADER_TERM_PREFIX ) ) ).thenReturn( leaderRef );
        when( hzInstance.<String, UUID>getMap( eq( CLUSTER_UUID_DB_NAME_MAP ) ) ).thenReturn( uuidDBMap );

        // when
        Map<MemberId, RoleInfo> roleMap = HazelcastClusterTopology.getCoreRoles( hzInstance, new HashSet<>( members ) );

        // then
        assertEquals( "First member was expected to be leader.", RoleInfo.LEADER, roleMap.get( chosenLeaderId ) );
    }

}
