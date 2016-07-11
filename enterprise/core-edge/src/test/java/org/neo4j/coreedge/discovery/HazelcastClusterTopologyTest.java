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
package org.neo4j.coreedge.discovery;

import com.hazelcast.client.impl.MemberImpl;
import com.hazelcast.core.Member;
import com.hazelcast.nio.Address;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.catchup.storecopy.LocalDatabase;
import org.neo4j.coreedge.raft.replication.tx.ConstantTimeRetryStrategy;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.coreedge.server.edge.CoreServerSelectionStrategy;
import org.neo4j.coreedge.server.edge.EdgeServerStartupProcess;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.discovery.HazelcastClusterTopology.buildMemberAttributes;
import static org.neo4j.coreedge.discovery.HazelcastClusterTopology.extractMemberAttributes;

public class HazelcastClusterTopologyTest
{
    @Test
    public void edgeServersShouldRegisterThemselvesWithTheTopologyWhenTheyStart() throws Throwable
    {
        // given
        final Map<String, String> params = new HashMap<>();

        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).type.name(), "BOLT" );
        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).enabled.name(), "true" );
        params.put( new GraphDatabaseSettings.BoltConnector( "bolt" ).address.name(), "127.0.0.1:" + 8001 );

        Config config = new Config( params );

        final EdgeTopologyService topology = mock( EdgeTopologyService.class );

        // when

        final CoreServerSelectionStrategy connectionStrategy = mock( CoreServerSelectionStrategy.class );
        when( connectionStrategy.coreServer() ).thenReturn( new CoreMember( UUID.randomUUID() ) );

        LocalDatabase localDatabase = mock( LocalDatabase.class );
        when( localDatabase.isEmpty() ).thenReturn( true );
        final EdgeServerStartupProcess startupProcess = new EdgeServerStartupProcess( null,
                localDatabase,
                mock( Lifecycle.class ),
                mock( DataSourceManager.class ),
                connectionStrategy,
                new ConstantTimeRetryStrategy( 1, TimeUnit.MILLISECONDS ),
                NullLogProvider.getInstance(), topology, config );

        startupProcess.start();

        // then
        verify( topology ).registerEdgeServer( anyObject() );
    }

    @Test
    public void shouldStoreMemberIdentityAndAddressesAsMemberAttributes() throws Exception
    {
        // given
        CoreMember coreMember = new CoreMember( UUID.randomUUID() );
        Config config = Config.defaults();
        HashMap<String, String> settings = new HashMap<>();
        settings.put( CoreEdgeClusterSettings.transaction_advertised_address.name(), "tx:1001" );
        settings.put( CoreEdgeClusterSettings.raft_advertised_address.name(), "raft:2001" );
        settings.put( GraphDatabaseSettings.bolt_advertised_address.name(), "bolt:3001" );
        config.augment( settings );

        // when
        Map<String, Object> attributes = buildMemberAttributes( coreMember, config ).getAttributes();
        Pair<CoreMember, CoreAddresses> extracted = extractMemberAttributes( new MemberImpl( null, null, attributes ) );

        // then
        assertEquals( coreMember, extracted.first() );
        CoreAddresses addresses = extracted.other();
        assertEquals( new AdvertisedSocketAddress( "tx:1001" ), addresses.getCoreServer() );
        assertEquals( new AdvertisedSocketAddress( "raft:2001" ), addresses.getRaftServer() );
        assertEquals( new AdvertisedSocketAddress( "bolt:3001" ), addresses.getBoltServer() );
    }

    @Test
    public void shouldCollectMembersAsAMap() throws Exception
    {
        // given
        Set<Member> hazelcastMembers = new HashSet<>();
        List<CoreMember> coreMembers = new ArrayList<>();
        for ( int i = 0; i < 5; i++ )
        {
            CoreMember coreMember = new CoreMember( UUID.randomUUID() );
            coreMembers.add( coreMember );
            Config config = Config.defaults();
            HashMap<String, String> settings = new HashMap<>();
            settings.put( CoreEdgeClusterSettings.transaction_advertised_address.name(), "tx:" + (i + 1 ));
            settings.put( CoreEdgeClusterSettings.raft_advertised_address.name(), "raft:" + (i + 1 ));
            settings.put( GraphDatabaseSettings.bolt_advertised_address.name(), "bolt:" + (i + 1));
            config.augment( settings );
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null,
                    buildMemberAttributes( coreMember, config ).getAttributes() ) );
        }

        // when
        Map<CoreMember, CoreAddresses> coreMemberMap =
                HazelcastClusterTopology.toCoreMemberMap( hazelcastMembers, NullLog.getInstance() );

        // then
        for ( int i = 0; i < 5; i++ )
        {
            CoreAddresses coreAddresses = coreMemberMap.get( coreMembers.get( i ) );
            assertEquals( new AdvertisedSocketAddress( "tx:" + (i  + 1 )), coreAddresses.getCoreServer() );
            assertEquals( new AdvertisedSocketAddress( "raft:" + (i  + 1 )), coreAddresses.getRaftServer() );
            assertEquals( new AdvertisedSocketAddress( "bolt:" + (i  + 1 )), coreAddresses.getBoltServer() );
        }
    }

    @Test
    public void shouldLogAndExcludeMembersWithMissingAttributes() throws Exception
    {
        // given
        Set<Member> hazelcastMembers = new HashSet<>();
        List<CoreMember> coreMembers = new ArrayList<>();
        for ( int i = 0; i < 4; i++ )
        {
            CoreMember coreMember = new CoreMember( UUID.randomUUID() );
            coreMembers.add( coreMember );
            Map<String, Object> attributes = buildMemberAttributes( coreMember, Config.defaults() ).getAttributes();
            if ( i == 2 )
            {
                attributes.remove( HazelcastClusterTopology.RAFT_SERVER );
            }
            hazelcastMembers.add( new MemberImpl( new Address( "localhost", i ), null, attributes ) );
        }
        // when
        Map<CoreMember, CoreAddresses> map =
                HazelcastClusterTopology.toCoreMemberMap( hazelcastMembers, NullLog.getInstance() );

        // then
        assertThat( map.keySet(), hasItems( coreMembers.get( 0 ), coreMembers.get( 1 ), coreMembers.get( 3 ) ) );
        assertThat( map.keySet(), not( hasItems( coreMembers.get( 2 ) ) ) );
    }
}
