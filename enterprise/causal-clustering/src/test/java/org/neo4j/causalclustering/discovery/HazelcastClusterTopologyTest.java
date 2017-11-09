/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiMap;
import com.hazelcast.nio.Address;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.buildMemberAttributesForCore;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.toCoreMemberMap;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class HazelcastClusterTopologyTest
{
    private static final Set<String> GROUPS = asSet( "group1", "group2", "group3" );

    private final HazelcastInstance hzInstance = mock( HazelcastInstance.class );

    @Before
    public void setup()
    {
        @SuppressWarnings( "unchecked" )
        MultiMap<String,String> serverGroupsMMap = mock( MultiMap.class );
        when( serverGroupsMMap.get( any() ) ).thenReturn( GROUPS );
        when( hzInstance.getMultiMap( anyString() ) ).thenReturn( (MultiMap) serverGroupsMMap );
    }

    @Test
    public void shouldCollectMembersAsAMap() throws Exception
    {
        // given
        Set<Member> hazelcastMembers = new HashSet<>();
        List<MemberId> coreMembers = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            MemberId memberId = new MemberId( UUID.randomUUID() );
            coreMembers.add( memberId );
            Config config = Config.defaults();
            HashMap<String, String> settings = new HashMap<>();
            settings.put( CausalClusteringSettings.transaction_advertised_address.name(), "tx:" + (i + 1) );
            settings.put( CausalClusteringSettings.raft_advertised_address.name(), "raft:" + (i + 1) );
            settings.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
            settings.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
            settings.put( new BoltConnector( "bolt" ).advertised_address.name(), "bolt:" + (i + 1) );
            settings.put( new BoltConnector( "http" ).type.name(), "HTTP" );
            settings.put( new BoltConnector( "http" ).enabled.name(), "true" );
            settings.put( new BoltConnector( "http" ).advertised_address.name(), "http:" + (i + 1) );

            config.augment( settings );
            Map<String, Object> attributes = buildMemberAttributesForCore( memberId, config ).getAttributes();
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null, attributes, false ) );

        }

        // when
        Map<MemberId,CoreServerInfo> coreMemberMap = toCoreMemberMap( hazelcastMembers, NullLog.getInstance(), hzInstance );

        // then
        for ( int i = 0; i < 5; i++ )
        {
            CoreServerInfo coreServerInfo = coreMemberMap.get( coreMembers.get( i ) );
            assertEquals( new AdvertisedSocketAddress( "tx", i + 1 ), coreServerInfo.getCatchupServer() );
            assertEquals( new AdvertisedSocketAddress( "raft", i + 1 ), coreServerInfo.getRaftServer() );
            assertEquals( new AdvertisedSocketAddress( "bolt", i + 1 ), coreServerInfo.connectors().boltAddress() );
            assertEquals( coreServerInfo.groups(), GROUPS );
        }
    }

    @Test
    public void shouldLogAndExcludeMembersWithMissingAttributes() throws Exception
    {
        // given
        Set<Member> hazelcastMembers = new HashSet<>();
        List<MemberId> coreMembers = new ArrayList<>();
        for ( int i = 0; i < 4; i++ )
        {
            MemberId memberId = new MemberId( UUID.randomUUID() );
            coreMembers.add( memberId );
            Config config = Config.defaults();
            HashMap<String, String> settings = new HashMap<>();
            settings.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
            settings.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
            settings.put( new BoltConnector( "bolt" ).advertised_address.name(), "bolt:" + (i + 1) );
            settings.put( new BoltConnector( "http" ).type.name(), "HTTP" );
            settings.put( new BoltConnector( "http" ).enabled.name(), "true" );
            settings.put( new BoltConnector( "http" ).advertised_address.name(), "http:" + (i + 1) );

            config.augment( settings );
            Map<String, Object> attributes = buildMemberAttributesForCore( memberId, config ).getAttributes();
            if ( i == 2 )
            {
                attributes.remove( HazelcastClusterTopology.RAFT_SERVER );
            }
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null, attributes, false ) );
        }
        // when
        Map<MemberId,CoreServerInfo> map = toCoreMemberMap( hazelcastMembers, NullLog.getInstance(), hzInstance );

        // then
        assertThat( map.keySet(), hasItems( coreMembers.get( 0 ), coreMembers.get( 1 ), coreMembers.get( 3 ) ) );
        assertThat( map.keySet(), not( hasItems( coreMembers.get( 2 ) ) ) );
    }
}
