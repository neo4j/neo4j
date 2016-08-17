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

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.query.Predicate;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.coreedge.core.consensus.schedule.ControlledRenewableTimeoutService;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.startsWith;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.coreedge.discovery.HazelcastClient.REFRESH_EDGE;
import static org.neo4j.coreedge.discovery.HazelcastClusterTopology.BOLT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastClusterTopology.MEMBER_UUID;
import static org.neo4j.coreedge.discovery.HazelcastClusterTopology.RAFT_SERVER;
import static org.neo4j.coreedge.discovery.HazelcastClusterTopology.TRANSACTION_SERVER;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class HazelcastClientTest
{
    private static final AdvertisedSocketAddress ADDRESS = new AdvertisedSocketAddress( "localhost:7000" );

    @Test
    public void shouldReturnTopologyUsingHazelcastMembers() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance(), ADDRESS, new
                ControlledRenewableTimeoutService(), 60_000, 5_000 );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        when( hazelcastInstance.getAtomicReference( anyString() ) ).thenReturn( mock( IAtomicReference.class ) );
        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );
        when( hazelcastInstance.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );

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
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance(), ADDRESS, new
                ControlledRenewableTimeoutService(), 60_000, 5_000 );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        when( hazelcastInstance.getAtomicReference( anyString() ) ).thenReturn( mock( IAtomicReference.class ) );
        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );
        when( hazelcastInstance.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );

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
        when( hazelcastInstance.getAtomicReference( anyString() ) ).thenReturn( mock( IAtomicReference.class ) );
        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );

        HazelcastClient client = new HazelcastClient( connector, logProvider, ADDRESS, new
                ControlledRenewableTimeoutService(), 60_000, 5_000 );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        ClusterTopology topology = client.currentTopology();

        assertEquals( 0, topology.coreMembers().size() );
        verify( log ).info( startsWith( "Failed to read cluster topology from Hazelcast." ),
                any( IllegalStateException.class ) );
    }

    @Test
    public void shouldReturnEmptyTopologyIfInitiallyConnectedToHazelcastButItsNowUnavailable() throws Exception
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance(), ADDRESS, new
                ControlledRenewableTimeoutService(), 60_000, 5_000 );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );

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
        HazelcastClient client = new HazelcastClient( connector, NullLogProvider.getInstance(), ADDRESS, new
                ControlledRenewableTimeoutService(), 60_000, 5_000 );

        HazelcastInstance hazelcastInstance1 = mock( HazelcastInstance.class );
        HazelcastInstance hazelcastInstance2 = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance1 )
                .thenReturn( hazelcastInstance2 );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance1.getCluster() ).thenReturn( cluster )
                .thenThrow( new HazelcastInstanceNotActiveException() );
        when( hazelcastInstance2.getCluster() ).thenReturn( cluster );

        when( hazelcastInstance1.getAtomicReference( anyString() ) ).thenReturn( mock( IAtomicReference.class ) );
        when( hazelcastInstance1.getSet( anyString() ) ).thenReturn( new HazelcastSet() );
        when( hazelcastInstance2.getAtomicReference( anyString() ) ).thenReturn( mock( IAtomicReference.class ) );
        when( hazelcastInstance2.getSet( anyString() ) ).thenReturn( new HazelcastSet() );

        when( hazelcastInstance1.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );
        when( hazelcastInstance2.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );

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
    public void shouldRegisterEdgeServerInTopology() throws Throwable
    {
        // given
        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        Set<Member> members = asSet( makeMember( 1 ) );
        when( cluster.getMembers() ).thenReturn( members );

        Endpoint endpoint = mock( Endpoint.class );
        when( endpoint.getUuid() ).thenReturn( "12345" );

        Client client = mock( Client.class );
        final String clientId = "12345";
        when( client.getUuid() ).thenReturn( clientId );

        ClientService clientService = mock( ClientService.class );
        when( clientService.getConnectedClients() ).thenReturn( asSet( client ) );

        HazelcastMap hazelcastMap = new HazelcastMap();

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( hazelcastInstance.getAtomicReference( anyString() ) ).thenReturn( mock( IAtomicReference.class ) );
        when( hazelcastInstance.getMap( anyString() ) ).thenReturn( hazelcastMap );
        when( hazelcastInstance.getLocalEndpoint() ).thenReturn( endpoint );
        when( hazelcastInstance.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );
        when( hazelcastInstance.getClientService() ).thenReturn( clientService );

        HazelcastConnector connector = mock( HazelcastConnector.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        ControlledRenewableTimeoutService renewableTimeoutService = new ControlledRenewableTimeoutService();
        HazelcastClient hazelcastClient = new HazelcastClient( connector, NullLogProvider.getInstance(), ADDRESS,
                renewableTimeoutService, 60_000, 5_000 );

        hazelcastClient.start();
        renewableTimeoutService.invokeTimeout( REFRESH_EDGE );

        // when
        ClusterTopology clusterTopology = hazelcastClient.currentTopology();

        // then
        assertEquals( 1, clusterTopology.edgeMemberAddresses().size() );
    }

    @Test
    public void shouldRemoveEdgeServersOnGracefulShutdown() throws Throwable
    {
        // given
        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        Set<Member> members = asSet( makeMember( 1 ) );
        when( cluster.getMembers() ).thenReturn( members );

        Endpoint endpoint = mock( Endpoint.class );
        when( endpoint.getUuid() ).thenReturn( "12345" );

        Client client = mock( Client.class );
        final String clientId = "12345";
        when( client.getUuid() ).thenReturn( clientId );

        ClientService clientService = mock( ClientService.class );
        when( clientService.getConnectedClients() ).thenReturn( asSet( client ) );

        HazelcastMap hazelcastMap = new HazelcastMap();

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( hazelcastInstance.getAtomicReference( anyString() ) ).thenReturn( mock( IAtomicReference.class ) );
        when( hazelcastInstance.getMap( anyString() ) ).thenReturn( hazelcastMap );
        when( hazelcastInstance.getLocalEndpoint() ).thenReturn( endpoint );
        when( hazelcastInstance.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );
        when( hazelcastInstance.getClientService() ).thenReturn( clientService );

        HazelcastConnector connector = mock( HazelcastConnector.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        ControlledRenewableTimeoutService renewableTimeoutService = new ControlledRenewableTimeoutService();
        HazelcastClient hazelcastClient = new HazelcastClient( connector, NullLogProvider.getInstance(), ADDRESS,
                renewableTimeoutService, 60_000, 5_000 );

        hazelcastClient.start();
        renewableTimeoutService.invokeTimeout( REFRESH_EDGE );

        int numberOfStartedEdgeServers = hazelcastClient.currentTopology().edgeMemberAddresses().size();

        // when
        hazelcastClient.stop();

        // then
        assertEquals( 0, numberOfStartedEdgeServers - 1 );
    }

    private Member makeMember( int id ) throws UnknownHostException
    {
        Member member = mock( Member.class );
        when( member.getStringAttribute( MEMBER_UUID ) ).thenReturn( UUID.randomUUID().toString() );
        when( member.getStringAttribute( TRANSACTION_SERVER ) ).thenReturn( format( "host%d:%d", id, (7000 + id) ) );
        when( member.getStringAttribute( RAFT_SERVER ) ).thenReturn( format( "host%d:%d", id, (6000 + id) ) );
        when( member.getStringAttribute( BOLT_SERVER ) ).thenReturn( format( "host%d:%d", id, (5000 + id) ) );
        return member;
    }

    private class HazelcastMap implements IMap<Object,Object>
    {
        private HashMap delegate = new HashMap();

        @Override
        public int size()
        {
            return delegate.size();
        }

        @Override
        public boolean isEmpty()
        {
            return delegate.isEmpty();
        }

        @Override
        public Object get( Object key )
        {

            return delegate.get( key );
        }

        @Override
        public boolean containsKey( Object key )
        {
            return delegate.containsKey( key );
        }

        @Override
        public Object put( Object key, Object value )
        {
            return delegate.put( key, value );
        }

        @Override
        public void putAll( Map m )
        {
            delegate.putAll( m );
        }

        @Override
        public Object remove( Object key )
        {
            return delegate.remove( key );
        }

        @Override
        public void clear()
        {
            delegate.clear();
        }

        @Override
        public Future getAsync( Object key )
        {
            return null;
        }

        @Override
        public Future putAsync( Object key, Object value )
        {
            return null;
        }

        @Override
        public Future putAsync( Object key, Object value, long ttl, TimeUnit timeunit )
        {
            return null;
        }

        @Override
        public Future removeAsync( Object key )
        {
            return null;
        }

        @Override
        public boolean tryRemove( Object key, long timeout, TimeUnit timeunit )
        {
            return false;
        }

        @Override
        public boolean tryPut( Object key, Object value, long timeout, TimeUnit timeunit )
        {
            return false;
        }

        @Override
        public Object put( Object key, Object value, long ttl, TimeUnit timeunit )
        {
            return delegate.put( key, value );
        }

        @Override
        public void putTransient( Object key, Object value, long ttl, TimeUnit timeunit )
        {

        }

        @Override
        public boolean containsValue( Object value )
        {
            return delegate.containsValue( value );
        }

        @Override
        public Set<Object> keySet()
        {
            return delegate.keySet();
        }

        @Override
        public Collection<Object> values()
        {
            return delegate.values();
        }

        @Override
        public Set<Entry<Object,Object>> entrySet()
        {
            return delegate.entrySet();
        }

        @Override
        public Set<Object> keySet( Predicate predicate )
        {
            return null;
        }

        @Override
        public Set<Map.Entry<Object,Object>> entrySet( Predicate predicate )
        {
            return null;
        }

        @Override
        public Collection values( Predicate predicate )
        {
            return null;
        }

        @Override
        public Set<Object> localKeySet()
        {
            return null;
        }

        @Override
        public Set<Object> localKeySet( Predicate predicate )
        {
            return null;
        }

        @Override
        public void addIndex( String attribute, boolean ordered )
        {

        }

        @Override
        public LocalMapStats getLocalMapStats()
        {
            return null;
        }

        @Override
        public Object executeOnKey( Object key, EntryProcessor entryProcessor )
        {
            return null;
        }

        @Override
        public void submitToKey( Object key, EntryProcessor entryProcessor, ExecutionCallback callback )
        {

        }

        @Override
        public Future submitToKey( Object key, EntryProcessor entryProcessor )
        {
            return null;
        }

        @Override
        public Map<Object,Object> executeOnEntries( EntryProcessor entryProcessor )
        {
            return null;
        }

        @Override
        public Map<Object,Object> executeOnEntries( EntryProcessor entryProcessor, Predicate predicate )
        {
            return null;
        }

        @Override
        public Object aggregate( Supplier supplier, Aggregation aggregation, JobTracker jobTracker )
        {
            return null;
        }

        @Override
        public Object aggregate( Supplier supplier, Aggregation aggregation )
        {
            return null;
        }

        @Override
        public Map<Object,Object> executeOnKeys( Set keys, EntryProcessor entryProcessor )
        {
            return null;
        }

        @Override
        public Object getOrDefault( Object key, Object defaultValue )
        {
            return delegate.getOrDefault( key, defaultValue );
        }

        @Override
        public Object putIfAbsent( Object key, Object value )
        {
            return delegate.putIfAbsent( key, value );
        }

        @Override
        public Object putIfAbsent( Object key, Object value, long ttl, TimeUnit timeunit )
        {
            return null;
        }

        @Override
        public boolean remove( Object key, Object value )
        {
            return delegate.remove( key, value );
        }

        @Override
        public void delete( Object key )
        {

        }

        @Override
        public void flush()
        {

        }

        @Override
        public void loadAll( boolean replaceExistingValues )
        {

        }

        @Override
        public void loadAll( Set keys, boolean replaceExistingValues )
        {

        }

        @Override
        public Map getAll( Set keys )
        {
            return null;
        }

        @Override
        public boolean replace( Object key, Object oldValue, Object newValue )
        {
            return delegate.replace( key, oldValue, newValue );
        }

        @Override
        public Object replace( Object key, Object value )
        {
            return delegate.replace( key, value );
        }

        @Override
        public void set( Object key, Object value )
        {

        }

        @Override
        public void set( Object key, Object value, long ttl, TimeUnit timeunit )
        {

        }

        @Override
        public void lock( Object key )
        {

        }

        @Override
        public void lock( Object key, long leaseTime, TimeUnit timeUnit )
        {

        }

        @Override
        public boolean isLocked( Object key )
        {
            return false;
        }

        @Override
        public boolean tryLock( Object key )
        {
            return false;
        }

        @Override
        public boolean tryLock( Object key, long time, TimeUnit timeunit ) throws InterruptedException
        {
            return false;
        }

        @Override
        public boolean tryLock( Object key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseTimeunit )
                throws InterruptedException
        {
            return false;
        }

        @Override
        public void unlock( Object key )
        {

        }

        @Override
        public void forceUnlock( Object key )
        {

        }

        @Override
        public String addLocalEntryListener( MapListener listener )
        {
            return null;
        }

        @Override
        public String addLocalEntryListener( EntryListener listener )
        {
            return null;
        }

        @Override
        public String addLocalEntryListener( MapListener listener, Predicate predicate, boolean includeValue )
        {
            return null;
        }

        @Override
        public String addLocalEntryListener( EntryListener listener, Predicate predicate, boolean includeValue )
        {
            return null;
        }

        @Override
        public String addLocalEntryListener( MapListener listener, Predicate predicate, Object key, boolean
                includeValue )
        {
            return null;
        }

        @Override
        public String addLocalEntryListener( EntryListener listener, Predicate predicate, Object key, boolean
                includeValue )
        {
            return null;
        }

        @Override
        public String addInterceptor( MapInterceptor interceptor )
        {
            return null;
        }

        @Override
        public void removeInterceptor( String id )
        {

        }

        @Override
        public String addEntryListener( MapListener listener, boolean includeValue )
        {
            return null;
        }

        @Override
        public String addEntryListener( EntryListener listener, boolean includeValue )
        {
            return null;
        }

        @Override
        public boolean removeEntryListener( String id )
        {
            return false;
        }

        @Override
        public String addPartitionLostListener( MapPartitionLostListener listener )
        {
            return null;
        }

        @Override
        public boolean removePartitionLostListener( String id )
        {
            return false;
        }

        @Override
        public String addEntryListener( MapListener listener, Object key, boolean includeValue )
        {
            return null;
        }

        @Override
        public String addEntryListener( EntryListener listener, Object key, boolean includeValue )
        {
            return null;
        }

        @Override
        public String addEntryListener( MapListener listener, Predicate predicate, boolean includeValue )
        {
            return null;
        }

        @Override
        public String addEntryListener( EntryListener listener, Predicate predicate, boolean includeValue )
        {
            return null;
        }

        @Override
        public String addEntryListener( MapListener listener, Predicate predicate, Object key, boolean includeValue )
        {
            return null;
        }

        @Override
        public String addEntryListener( EntryListener listener, Predicate predicate, Object key, boolean includeValue )
        {
            return null;
        }

        @Override
        public EntryView getEntryView( Object key )
        {
            return null;
        }

        @Override
        public boolean evict( Object key )
        {
            return false;
        }

        @Override
        public void evictAll()
        {

        }

        @Override
        public Object computeIfAbsent( Object key, Function mappingFunction )
        {
            return delegate.computeIfAbsent( key, mappingFunction );
        }

        @Override
        public Object computeIfPresent( Object key, BiFunction remappingFunction )
        {
            return delegate.computeIfPresent( key, remappingFunction );
        }

        @Override
        public Object compute( Object key, BiFunction remappingFunction )
        {
            return delegate.compute( key, remappingFunction );
        }

        @Override
        public Object merge( Object key, Object value, BiFunction remappingFunction )
        {
            return delegate.merge( key, value, remappingFunction );
        }

        @Override
        public void forEach( BiConsumer action )
        {
            delegate.forEach( action );
        }

        @Override
        public void replaceAll( BiFunction function )
        {
            delegate.replaceAll( function );
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

        @Override
        public String toString()
        {
            return delegate.toString();
        }

        @Override
        public String getPartitionKey()
        {
            return null;
        }

        @Override
        public String getName()
        {
            return "name";
        }

        @Override
        public String getServiceName()
        {
            return "serviceName";
        }

        @Override
        public void destroy()
        {

        }
    }

    private class HazelcastSet implements ISet<Object>
    {
        private Set<Object> delegate;

        public HazelcastSet()
        {
            this.delegate = new HashSet<>();
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

    private class StubExecutorService implements IExecutorService
    {
        private ExecutorService executor = Executors.newSingleThreadExecutor();

        @Override
        public void execute( Runnable command, MemberSelector memberSelector )
        {

        }

        @Override
        public void executeOnKeyOwner( Runnable command, Object key )
        {

        }

        @Override
        public void executeOnMember( Runnable command, Member member )
        {

        }

        @Override
        public void executeOnMembers( Runnable command, Collection<Member> members )
        {

        }

        @Override
        public void executeOnMembers( Runnable command, MemberSelector memberSelector )
        {

        }

        @Override
        public void executeOnAllMembers( Runnable command )
        {

        }

        @Override
        public <T> Future<T> submit( Callable<T> task, MemberSelector memberSelector )
        {
            return null;
        }

        @Override
        public <T> Future<T> submitToKeyOwner( Callable<T> task, Object key )
        {
            return null;
        }

        @Override
        public <T> Future<T> submitToMember( Callable<T> task, Member member )
        {
            return null;
        }

        @Override
        public <T> Map<Member,Future<T>> submitToMembers( Callable<T> task, Collection<Member> members )
        {
            return null;
        }

        @Override
        public <T> Map<Member,Future<T>> submitToMembers( Callable<T> task, MemberSelector memberSelector )
        {
            return null;
        }

        @Override
        public <T> Map<Member,Future<T>> submitToAllMembers( Callable<T> task )
        {
            return null;
        }

        @Override
        public <T> void submit( Runnable task, ExecutionCallback<T> callback )
        {

        }

        @Override
        public <T> void submit( Runnable task, MemberSelector memberSelector, ExecutionCallback<T> callback )
        {

        }

        @Override
        public <T> void submitToKeyOwner( Runnable task, Object key, ExecutionCallback<T> callback )
        {

        }

        @Override
        public <T> void submitToMember( Runnable task, Member member, ExecutionCallback<T> callback )
        {

        }

        @Override
        public void submitToMembers( Runnable task, Collection<Member> members, MultiExecutionCallback callback )
        {

        }

        @Override
        public void submitToMembers( Runnable task, MemberSelector memberSelector, MultiExecutionCallback callback )
        {

        }

        @Override
        public void submitToAllMembers( Runnable task, MultiExecutionCallback callback )
        {

        }

        @Override
        public <T> void submit( Callable<T> task, ExecutionCallback<T> callback )
        {

        }

        @Override
        public <T> void submit( Callable<T> task, MemberSelector memberSelector, ExecutionCallback<T> callback )
        {

        }

        @Override
        public <T> void submitToKeyOwner( Callable<T> task, Object key, ExecutionCallback<T> callback )
        {

        }

        @Override
        public <T> void submitToMember( Callable<T> task, Member member, ExecutionCallback<T> callback )
        {

        }

        @Override
        public <T> void submitToMembers( Callable<T> task, Collection<Member> members, MultiExecutionCallback callback )
        {

        }

        @Override
        public <T> void submitToMembers( Callable<T> task, MemberSelector memberSelector, MultiExecutionCallback
                callback )
        {

        }

        @Override
        public <T> void submitToAllMembers( Callable<T> task, MultiExecutionCallback callback )
        {

        }

        @Override
        public LocalExecutorStats getLocalExecutorStats()
        {
            return null;
        }

        @Override
        public String getPartitionKey()
        {
            return null;
        }

        @Override
        public String getName()
        {
            return null;
        }

        @Override
        public String getServiceName()
        {
            return null;
        }

        @Override
        public void destroy()
        {

        }

        @Override
        public void shutdown()
        {

        }

        @Override
        public List<Runnable> shutdownNow()
        {
            return null;
        }

        @Override
        public boolean isShutdown()
        {
            return false;
        }

        @Override
        public boolean isTerminated()
        {
            return false;
        }

        @Override
        public boolean awaitTermination( long timeout, TimeUnit unit ) throws InterruptedException
        {
            return false;
        }

        @Override
        public <T> Future<T> submit( Callable<T> task )
        {
            return executor.submit( task );
        }

        @Override
        public <T> Future<T> submit( Runnable task, T result )
        {
            return null;
        }

        @Override
        public Future<?> submit( Runnable task )
        {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll( Collection<? extends Callable<T>> tasks ) throws InterruptedException
        {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll( Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit )
                throws InterruptedException
        {
            return null;
        }

        @Override
        public <T> T invokeAny( Collection<? extends Callable<T>> tasks ) throws InterruptedException,
                ExecutionException
        {
            return null;
        }

        @Override
        public <T> T invokeAny( Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit ) throws
                InterruptedException, ExecutionException, TimeoutException
        {
            return null;
        }

        @Override
        public void execute( Runnable command )
        {

        }
    }
}
