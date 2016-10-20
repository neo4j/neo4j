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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import org.junit.Test;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLog;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.buildMemberAttributes;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.extractMemberAttributes;

public class HazelcastClusterTopologyTest
{
    @Test
    public void shouldStoreMemberIdentityAndAddressesAsMemberAttributes() throws Exception
    {
        // given
        MemberId memberId = new MemberId( UUID.randomUUID() );
        Config config = Config.defaults();
        HashMap<String, String> settings = new HashMap<>();
        settings.put( CausalClusteringSettings.transaction_advertised_address.name(), "tx:1001" );
        settings.put( CausalClusteringSettings.raft_advertised_address.name(), "raft:2001" );
        settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
        settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
        settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).advertised_address.name(), "bolt:3001" );
        settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).type.name(), "HTTP" );
        settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).enabled.name(), "true" );
        settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).advertised_address.name(), "http:3001" );
        config.augment( settings );

        // when
        Map<String, Object> attributes = buildMemberAttributes( memberId, config ).getAttributes();
        Pair<MemberId, CoreAddresses> extracted = extractMemberAttributes( new MemberImpl( null, null, attributes,
                false ) );

        // then
        assertEquals( memberId, extracted.first() );
        CoreAddresses addresses = extracted.other();
        assertEquals( new AdvertisedSocketAddress( "tx", 1001 ), addresses.getCatchupServer() );
        assertEquals( new AdvertisedSocketAddress( "raft", 2001 ), addresses.getRaftServer() );
        assertEquals( new AdvertisedSocketAddress( "bolt", 3001 ), addresses.getClientConnectorAddresses().getBoltAddress() );
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
            settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
            settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
            settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).advertised_address.name(), "bolt:" + (i + 1) );
            settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).type.name(), "HTTP" );
            settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).enabled.name(), "true" );
            settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).advertised_address.name(), "http:" + (i + 1) );

            config.augment( settings );
            Map<String, Object> attributes = buildMemberAttributes( memberId, config ).getAttributes();
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null, attributes, false ) );

        }

        // when
        Map<MemberId, CoreAddresses> coreMemberMap =
                HazelcastClusterTopology.toCoreMemberMap( hazelcastMembers, NullLog.getInstance() );

        // then
        for ( int i = 0; i < 5; i++ )
        {
            CoreAddresses coreAddresses = coreMemberMap.get( coreMembers.get( i ) );
            assertEquals( new AdvertisedSocketAddress( "tx", (i + 1) ), coreAddresses.getCatchupServer() );
            assertEquals( new AdvertisedSocketAddress( "raft", (i + 1) ), coreAddresses.getRaftServer() );
            assertEquals( new AdvertisedSocketAddress( "bolt", (i + 1) ), coreAddresses.getClientConnectorAddresses().getBoltAddress() );
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
            settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
            settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
            settings.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).advertised_address.name(), "bolt:" + (i + 1) );
            settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).type.name(), "HTTP" );
            settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).enabled.name(), "true" );
            settings.put( new GraphDatabaseSettings.BoltConnector( "http" ).advertised_address.name(), "http:" + (i + 1) );

            config.augment( settings );
            Map<String, Object> attributes = buildMemberAttributes( memberId, config ).getAttributes();
            if ( i == 2 )
            {
                attributes.remove( HazelcastClusterTopology.RAFT_SERVER );
            }
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null, attributes, false ) );
        }
        // when
        Map<MemberId, CoreAddresses> map =
                HazelcastClusterTopology.toCoreMemberMap( hazelcastMembers, NullLog.getInstance() );

        // then
        assertThat( map.keySet(), hasItems( coreMembers.get( 0 ), coreMembers.get( 1 ), coreMembers.get( 3 ) ) );
        assertThat( map.keySet(), not( hasItems( coreMembers.get( 2 ) ) ) );
    }
}
