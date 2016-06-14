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

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.Spliterator;

import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import org.junit.Test;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.AdvertisedSocketAddress;

import static java.lang.String.format;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
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

        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology = client.currentTopology();

        // then
        assertEquals( members.size(), topology.coreMembers().size() );
    }

    @Test
    public void shouldNotReconnectWhileHazelcastRemainsAvailable() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance() );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology;
        for ( int i = 0; i < 5; i++ )
        {
            topology = client.currentTopology();
            assertEquals( members.size(), topology.coreMembers().size() );
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

        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );

        HazelcastClient client = new HazelcastClient( connector, logProvider );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology = client.currentTopology();

        assertEquals( 0, topology.coreMembers().size() );
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

        when(hazelcastInstance.getSet( anyString() )).thenReturn(  new HazelcastSet() );

        when( hazelcastInstance.getCluster() ).thenThrow( new HazelcastInstanceNotActiveException() );

        // when
        ClusterTopology topology = client.currentTopology();

        // then
        assertEquals( 0, topology.coreMembers().size() );
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

        when(hazelcastInstance1.getSet( anyString() )).thenReturn(  new HazelcastSet() );
        when(hazelcastInstance2.getSet( anyString() )).thenReturn(  new HazelcastSet() );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology1 = client.currentTopology();

        // then
        assertEquals( members.size(), topology1.coreMembers().size() );

        // when
        ClusterTopology topology2 = client.currentTopology();

        // then
        assertEquals( members.size(), topology2.coreMembers().size() );
        verify( connector, times( 2 ) ).connectToHazelcast();
    }

    @Test
    public void shouldRegisterEdgeServerInTopology() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance() );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        final ISet<Object> set = new HazelcastSet(  );

        when( hazelcastInstance.getSet(anyString()) ).thenReturn( set );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        client.currentTopology();
        client.registerEdgeServer( new AdvertisedSocketAddress( "localhost:7000" ) );

        // then
        assertEquals( 1, client.currentTopology().edgeMembers().size() );
    }

    private Member makeMember( int id ) throws UnknownHostException
    {
        Member member = mock( Member.class );
        when( member.getStringAttribute( TRANSACTION_SERVER ) ).thenReturn( format( "host%d:%d", id, (7000 + id) ) );
        when( member.getStringAttribute( RAFT_SERVER ) ).thenReturn( format( "host%d:%d", id, (6000 + id) ) );
        return member;
    }

    private class HazelcastSet implements ISet<Object>
    {
        private Set<Object> delegate;

        public HazelcastSet(  )
        {
            this.delegate = new HashSet<>(  );
        }

        @Override
        public Object getId()
        {
            throw new IllegalStateException();
        }

        @Override
        public String getPartitionKey()
        {
            throw new IllegalStateException();
        }

        @Override
        public String getName()
        {
            throw new IllegalStateException();
        }

        @Override
        public String getServiceName()
        {
            throw new IllegalStateException();
        }

        @Override
        public void destroy()
        {
            throw new IllegalStateException();
        }

        @Override
        public String addItemListener( ItemListener<Object> listener, boolean includeValue )
        {
            throw new IllegalStateException();
        }

        @Override
        public boolean removeItemListener( String registrationId )
        {
            throw new IllegalStateException();
        }

        public int size()
        {
            return delegate.size();
        }

        public boolean isEmpty()
        {
            return delegate.isEmpty();
        }

        public boolean contains( Object o )
        {
            return delegate.contains( o );
        }

        public Iterator<Object> iterator()
        {
            return delegate.iterator();
        }

        public Object[] toArray()
        {
            return delegate.toArray();
        }

        public <T> T[] toArray( T[] a )
        {
            return delegate.toArray( a );
        }

        public boolean add( Object o )
        {
            return delegate.add( o );
        }

        public boolean remove( Object o )
        {
            return delegate.remove( o );
        }

        public boolean containsAll( Collection<?> c )
        {
            return delegate.containsAll( c );
        }

        public boolean addAll( Collection<?> c )
        {
            return delegate.addAll( c );
        }

        public boolean retainAll( Collection<?> c )
        {
            return delegate.retainAll( c );
        }

        public boolean removeAll( Collection<?> c )
        {
            return delegate.removeAll( c );
        }

        public void clear()
        {
            delegate.clear();
        }

        @Override
        public boolean equals( Object o )
        {
            return delegate.equals( o );
        }

        @Override
        public int hashCode()
        {
            return delegate.hashCode();
        }

        public Spliterator<Object> spliterator()
        {
            return delegate.spliterator();
        }
    }
}
