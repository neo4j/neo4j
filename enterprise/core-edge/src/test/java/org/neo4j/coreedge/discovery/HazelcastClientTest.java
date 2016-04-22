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

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.Member;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Set;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.RAFT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastServerLifecycle.TRANSACTION_SERVER;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class HazelcastClientTest
{
    @Test
    public void shouldReturnTopologyUsingHazelcastMembers() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance() );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology = client.currentTopology();

        // then
        assertEquals( members.size(), topology.getMembers().size() );
    }

    @Test
    public void shouldNotReconnectWhileHazelcastRemainsAvailable() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance() );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology;
        for ( int i = 0; i < 5; i++ )
        {
            topology = client.currentTopology();
            assertEquals( members.size(), topology.getMembers().size() );
        }

        // then
        verify( connector, times( 1 ) ).connectToHazelcast();
    }

    @Test
    public void shouldReturnEmptyTopologyIfUnableToConnectToHazelcast() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        LogProvider logProvider = mock( LogProvider.class );

        Log log = mock( Log.class );
        when( logProvider.getLog( any( Class.class ) ) ).thenReturn( log );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenThrow( new IllegalStateException() );

        HazelcastClient client = new HazelcastClient( connector, logProvider );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology = client.currentTopology();

        assertEquals( 0, topology.getMembers().size() );
        verify( log ).info( "Unable to connect to core cluster" );
    }

    @Test
    public void shouldReturnEmptyTopologyIfInitiallyConnectedToHazelcastButItsNowUnavailable() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance() );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        when( hazelcastInstance.getCluster() ).thenThrow( new HazelcastInstanceNotActiveException() );

        // when
        ClusterTopology topology = client.currentTopology();

        // then
        assertEquals( 0, topology.getMembers().size() );
    }

    @Test
    public void shouldReconnectIfHazelcastUnavailable() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance() );

        HazelcastInstance hazelcastInstance1 = mock( HazelcastInstance.class );
        HazelcastInstance hazelcastInstance2 = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance1 )
                .thenReturn( hazelcastInstance2 );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance1.getCluster() ).thenReturn( cluster )
                .thenThrow( new HazelcastInstanceNotActiveException() );
        when( hazelcastInstance2.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology1 = client.currentTopology();

        // then
        assertEquals( members.size(), topology1.getMembers().size() );

        // when
        ClusterTopology topology2 = client.currentTopology();

        // then
        assertEquals( members.size(), topology2.getMembers().size() );
        verify( connector, times( 2 ) ).connectToHazelcast();
    }

    public Member makeMember( int id ) throws UnknownHostException
    {
        Member member = mock( Member.class );
        when( member.getStringAttribute( TRANSACTION_SERVER ) ).thenReturn( format( "host%d:%d", id, (7000 + id) ) );
        when( member.getStringAttribute( RAFT_SERVER ) ).thenReturn( format( "host%d:%d", id, (6000 + id) ) );
        return member;
    }
}
