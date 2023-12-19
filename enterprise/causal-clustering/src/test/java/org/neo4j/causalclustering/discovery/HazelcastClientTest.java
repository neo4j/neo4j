/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.discovery;

import com.hazelcast.core.Client;
import com.hazelcast.core.ClientService;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.Endpoint;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicReference;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.core.IMap;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberSelector;
import com.hazelcast.core.MultiExecutionCallback;
import com.hazelcast.core.MultiMap;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.MapInterceptor;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.aggregation.Aggregation;
import com.hazelcast.mapreduce.aggregation.Supplier;
import com.hazelcast.monitor.LocalExecutorStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalMultiMapStats;
import com.hazelcast.query.Predicate;
import org.junit.Test;

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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.CLIENT_CONNECTOR_ADDRESSES;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.MEMBER_DB_NAME;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.MEMBER_UUID;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.RAFT_SERVER;
import static org.neo4j.causalclustering.discovery.HazelcastClusterTopology.TRANSACTION_SERVER;
import static org.neo4j.helpers.collection.Iterators.asSet;

public class HazelcastClientTest
{
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private TopologyServiceRetryStrategy topologyServiceRetryStrategy = new TopologyServiceNoRetriesStrategy();
    private static final java.util.function.Supplier<HashMap<String, String>> DEFAULT_SETTINGS = () -> {
        HashMap<String, String> settings = new HashMap<>();

        settings.put( new BoltConnector( "bolt" ).type.name(), "BOLT" );
        settings.put( new BoltConnector( "bolt" ).enabled.name(), "true" );
        settings.put( new BoltConnector( "bolt" ).advertised_address.name(), "bolt:3001" );

        settings.put( new BoltConnector( "http" ).type.name(), "HTTP" );
        settings.put( new BoltConnector( "http" ).enabled.name(), "true" );
        settings.put( new BoltConnector( "http" ).advertised_address.name(), "http:3001" );
        return settings;
    };

    private Config config( HashMap<String, String> settings )
    {
        HashMap<String, String> defaults = DEFAULT_SETTINGS.get();
        defaults.putAll( settings );
        return Config.defaults( defaults );
    }

    private Config config( String key, String value )
    {
        HashMap<String, String> defaults = DEFAULT_SETTINGS.get();
        defaults.put(key, value);
        return Config.defaults( defaults );
    }

    private Config config()
    {

        return Config.defaults( DEFAULT_SETTINGS.get() );
    }

    private HazelcastClient hzClient( OnDemandJobScheduler jobScheduler, com.hazelcast.core.Cluster cluster, Config config )
    {
        HazelcastConnector connector = mock( HazelcastConnector.class );

        HazelcastClient client = new HazelcastClient( connector, jobScheduler, NullLogProvider.getInstance(), config, myself );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );
        when( hazelcastInstance.getMultiMap( anyString() ) ).thenReturn( new HazelcastMultiMap() );
        when( hazelcastInstance.getMap( anyString() ) ).thenReturn( new HazelcastMap() );

        when( hazelcastInstance.getCluster() ).thenReturn( cluster );
        when( hazelcastInstance.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );

        return client;
    }

    private HazelcastClient startedClientWithMembers( Set<Member> members, Config config )
    {
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        com.hazelcast.core.Cluster cluster = mock( Cluster.class );

        when( cluster.getMembers() ).thenReturn( members );

        HazelcastClient client = hzClient( jobScheduler, cluster, config );
        client.start();
        jobScheduler.runJob();

        return client;
    }

    @Test
    public void shouldReturnTopologyUsingHazelcastMembers()
    {
        // given
        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        HazelcastClient client = startedClientWithMembers( members, config() );

        // when
        CoreTopology topology = client.localCoreServers();

        // then
        assertEquals( members.size(), topology.members().size() );
    }

    @Test
    public void localAndAllTopologiesShouldMatchForSingleDBName()
    {
        // given
        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        HazelcastClient client = startedClientWithMembers( members, config() );

        // then
        String message = "Different local and global topologies reported despite single, default database name.";
        assertEquals(message, client.allCoreServers(), client.localCoreServers() );
    }

    @Test
    public void localAndAllTopologiesShouldDifferForMultipleDBNames()
    {
        // given
        Set<Member> members = asSet( makeMember( 1, "foo" ), makeMember( 2, "bar" ) );
        HazelcastClient client = startedClientWithMembers( members, config( CausalClusteringSettings.database.name(), "foo" ) );

        // then
        String message = "Identical local and global topologies reported despite multiple, distinct database names.";
        assertNotEquals(message, client.allCoreServers(), client.localCoreServers() );
        assertEquals( 1, client.localCoreServers().members().size() );
    }

    @Test
    public void allTopologyShouldContainAllMembers()
    {
        // given
        Set<Member> members = asSet( makeMember( 1, "foo" ), makeMember( 2, "bar" ) );
        HazelcastClient client = startedClientWithMembers( members, config( CausalClusteringSettings.database.name(), "foo" ) );

        // then
        String message = "Global topology should contain all Hazelcast Members despite different db names.";
        assertEquals(message, members.size(), client.allCoreServers().members().size() );
    }

    @Test
    public void shouldNotReconnectWhileHazelcastRemainsAvailable()
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();

        HazelcastClient client = new HazelcastClient( connector, jobScheduler, NullLogProvider.getInstance(), config(), myself );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );
        when( hazelcastInstance.getMultiMap( anyString() ) ).thenReturn( new HazelcastMultiMap() );
        when( hazelcastInstance.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );
        when( hazelcastInstance.getMap( anyString() ) ).thenReturn( new HazelcastMap() );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        client.start();
        jobScheduler.runJob();

        CoreTopology topology;
        for ( int i = 0; i < 5; i++ )
        {
            topology = client.allCoreServers();
            assertEquals( members.size(), topology.members().size() );
        }

        // then
        verify( connector, times( 1 ) ).connectToHazelcast();
    }

    @Test
    public void shouldReturnEmptyTopologyIfUnableToConnectToHazelcast()
    {
        // given
        HazelcastConnector connector = mock( HazelcastConnector.class );
        LogProvider logProvider = mock( LogProvider.class );

        Log log = mock( Log.class );
        when( logProvider.getLog( any( Class.class ) ) ).thenReturn( log );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( connector.connectToHazelcast() ).thenThrow( new IllegalStateException() );
        IAtomicReference iAtomicReference = mock( IAtomicReference.class );
        when( hazelcastInstance.getAtomicReference( anyString() ) ).thenReturn( iAtomicReference );
        when( hazelcastInstance.getSet( anyString() ) ).thenReturn( new HazelcastSet() );

        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();

        HazelcastClient client = new HazelcastClient( connector, jobScheduler, logProvider, config(), myself );

        com.hazelcast.core.Cluster cluster = mock( Cluster.class );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );

        Set<Member> members = asSet( makeMember( 1 ), makeMember( 2 ) );
        when( cluster.getMembers() ).thenReturn( members );

        // when
        client.start();
        jobScheduler.runJob();
        CoreTopology topology = client.allCoreServers();

        assertEquals( 0, topology.members().size() );
    }

    @Test
    public void shouldRegisterReadReplicaInTopology()
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
        HazelcastMultiMap hazelcastMultiMap = new HazelcastMultiMap();

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        IAtomicReference iAtomicReference = mock( IAtomicReference.class );
        when( hazelcastInstance.getAtomicReference( anyString() ) ).thenReturn( iAtomicReference );
        when( hazelcastInstance.getMap( anyString() ) ).thenReturn( hazelcastMap );
        when( hazelcastInstance.getMultiMap( anyString() ) ).thenReturn( hazelcastMultiMap );
        when( hazelcastInstance.getLocalEndpoint() ).thenReturn( endpoint );
        when( hazelcastInstance.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );
        when( hazelcastInstance.getClientService() ).thenReturn( clientService );

        HazelcastConnector connector = mock( HazelcastConnector.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        HazelcastClient hazelcastClient = new HazelcastClient( connector, jobScheduler, NullLogProvider.getInstance(), config(), myself );

        // when
        hazelcastClient.start();
        jobScheduler.runJob();

        // then
        assertEquals( 1, hazelcastMap.size() );
    }

    @Test
    public void shouldRemoveReadReplicasOnGracefulShutdown()
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
        IAtomicReference iAtomicReference = mock( IAtomicReference.class );
        when( hazelcastInstance.getAtomicReference( anyString() ) ).thenReturn( iAtomicReference );
        when( hazelcastInstance.getMap( anyString() ) ).thenReturn( hazelcastMap );
        when( hazelcastInstance.getMultiMap( anyString() ) ).thenReturn( new HazelcastMultiMap() );
        when( hazelcastInstance.getLocalEndpoint() ).thenReturn( endpoint );
        when( hazelcastInstance.getExecutorService( anyString() ) ).thenReturn( new StubExecutorService() );
        when( hazelcastInstance.getCluster() ).thenReturn( cluster );
        when( hazelcastInstance.getClientService() ).thenReturn( clientService );

        HazelcastConnector connector = mock( HazelcastConnector.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();
        HazelcastClient hazelcastClient = new HazelcastClient( connector, jobScheduler, NullLogProvider.getInstance(), config(), myself );

        hazelcastClient.start();

        jobScheduler.runJob();

        // when
        hazelcastClient.stop();

        // then
        assertEquals( 0, hazelcastMap.size() );
    }

    @Test
    public void shouldSwallowNPEFromHazelcast()
    {
        // given
        Endpoint endpoint = mock( Endpoint.class );
        when( endpoint.getUuid() ).thenReturn( "12345" );

        HazelcastInstance hazelcastInstance = mock( HazelcastInstance.class );
        when( hazelcastInstance.getLocalEndpoint() ).thenReturn( endpoint );
        when( hazelcastInstance.getMap( anyString() ) ).thenReturn( new HazelcastMap() );
        when( hazelcastInstance.getMultiMap( anyString() ) ).thenReturn( new HazelcastMultiMap() );
        doThrow( new NullPointerException( "boom!!!" ) ).when( hazelcastInstance ).shutdown();

        HazelcastConnector connector = mock( HazelcastConnector.class );
        when( connector.connectToHazelcast() ).thenReturn( hazelcastInstance );

        OnDemandJobScheduler jobScheduler = new OnDemandJobScheduler();

        HazelcastClient hazelcastClient = new HazelcastClient( connector, jobScheduler, NullLogProvider.getInstance(), config(), myself );

        hazelcastClient.start();

        jobScheduler.runJob();

        // when
        hazelcastClient.stop();

        // then no NPE has been thrown
    }

    private Member makeMember( int id )
    {
        return makeMember( id, CausalClusteringSettings.database.getDefaultValue() );
    }

    private Member makeMember( int id, String databaseName )
    {
        Member member = mock( Member.class );
        when( member.getStringAttribute( MEMBER_UUID ) ).thenReturn( UUID.randomUUID().toString() );
        when( member.getStringAttribute( TRANSACTION_SERVER ) ).thenReturn( format( "host%d:%d", id, 7000 + id ) );
        when( member.getStringAttribute( RAFT_SERVER ) ).thenReturn( format( "host%d:%d", id, 6000 + id ) );
        when( member.getStringAttribute( CLIENT_CONNECTOR_ADDRESSES ) )
                .thenReturn( format( "bolt://host%d:%d,http://host%d:%d", id, 5000 + id, id, 5000 + id ) );
        when( member.getStringAttribute( MEMBER_DB_NAME ) ).thenReturn( databaseName );
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
        public ICompletableFuture<Object> getAsync( Object key )
        {
            return null;
        }

        @Override
        public ICompletableFuture<Object> putAsync( Object key, Object value )
        {
            return null;
        }

        @Override
        public ICompletableFuture<Object> putAsync( Object key, Object value, long ttl, TimeUnit timeunit )
        {
            return null;
        }

        @Override
        public ICompletableFuture<Void> setAsync( Object o, Object o2 )
        {
            return null;
        }

        @Override
        public ICompletableFuture<Void> setAsync( Object o, Object o2, long l, TimeUnit timeUnit )
        {
            return null;
        }

        @Override
        public ICompletableFuture<Object> removeAsync( Object key )
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
        public ICompletableFuture submitToKey( Object key, EntryProcessor entryProcessor )
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
        public boolean tryLock( Object key, long time, TimeUnit timeunit )
        {
            return false;
        }

        @Override
        public boolean tryLock( Object key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseTimeunit )
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

    private class HazelcastMultiMap implements MultiMap<Object,Object>
    {
        private Map<Object,Object> delegate = new HashMap<>();

        @Override
        public String getPartitionKey()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getName()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getServiceName()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean put( Object key, Object value )
        {
            if ( delegate.get( key ) != null )
            {
                throw new UnsupportedOperationException( "This is not a true multimap" );
            }
            delegate.put( key, value );
            return true;
        }

        @Override
        public Collection<Object> get( Object key )
        {
            return asSet( delegate.get( key ) );
        }

        @Override
        public boolean remove( Object key, Object value )
        {
            return delegate.remove( key, value );
        }

        @Override
        public Collection<Object> remove( Object key )
        {
            return asSet( delegate.remove( key ) );
        }

        @Override
        public Set<Object> localKeySet()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Object> keySet()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Collection<Object> values()
        {
            return delegate.values();
        }

        @Override
        public Set<Map.Entry<Object,Object>> entrySet()
        {
            return delegate.entrySet();
        }

        @Override
        public boolean containsKey( Object key )
        {
            return delegate.containsKey( key );
        }

        @Override
        public boolean containsValue( Object value )
        {
            return delegate.containsValue( value );
        }

        @Override
        public boolean containsEntry( Object key, Object value )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public int size()
        {
            return delegate.size();
        }

        @Override
        public void clear()
        {
            delegate.clear();
        }

        @Override
        public int valueCount( Object key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String addLocalEntryListener( EntryListener<Object,Object> listener )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String addEntryListener( EntryListener<Object,Object> listener, boolean includeValue )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeEntryListener( String registrationId )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public String addEntryListener( EntryListener<Object,Object> listener, Object key, boolean includeValue )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void lock( Object key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void lock( Object key, long leaseTime, TimeUnit timeUnit )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isLocked( Object key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock( Object key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock( Object key, long time, TimeUnit timeunit )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean tryLock( Object key, long time, TimeUnit timeunit, long leaseTime, TimeUnit leaseTimeunit )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void unlock( Object key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forceUnlock( Object key )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public LocalMultiMapStats getLocalMultiMapStats()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <SuppliedValue, Result> Result aggregate( Supplier<Object,Object,SuppliedValue> supplier,
                Aggregation<Object,SuppliedValue,Result> aggregation )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <SuppliedValue, Result> Result aggregate( Supplier<Object,Object,SuppliedValue> supplier,
                Aggregation<Object,SuppliedValue,Result> aggregation, JobTracker jobTracker )
        {
            throw new UnsupportedOperationException();
        }
    }

    private class HazelcastSet implements ISet<Object>
    {
        private Set<Object> delegate;

        HazelcastSet()
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
        public boolean contains( Object o )
        {
            return delegate.contains( o );
        }

        @Override
        public Iterator<Object> iterator()
        {
            return delegate.iterator();
        }

        @Override
        public Object[] toArray()
        {
            return delegate.toArray();
        }

        @Override
        public <T> T[] toArray( T[] a )
        {
            return delegate.toArray( a );
        }

        @Override
        public boolean add( Object o )
        {
            return delegate.add( o );
        }

        @Override
        public boolean remove( Object o )
        {
            return delegate.remove( o );
        }

        @Override
        public boolean containsAll( Collection<?> c )
        {
            return delegate.containsAll( c );
        }

        @Override
        public boolean addAll( Collection<?> c )
        {
            return delegate.addAll( c );
        }

        @Override
        public boolean retainAll( Collection<?> c )
        {
            return delegate.retainAll( c );
        }

        @Override
        public boolean removeAll( Collection<?> c )
        {
            return delegate.removeAll( c );
        }

        @Override
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

        @Override
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
        public boolean awaitTermination( long timeout, TimeUnit unit )
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
        public <T> List<Future<T>> invokeAll( Collection<? extends Callable<T>> tasks )
        {
            return null;
        }

        @Override
        public <T> List<Future<T>> invokeAll( Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit )
        {
            return null;
        }

        @Override
        public <T> T invokeAny( Collection<? extends Callable<T>> tasks )
        {
            return null;
        }

        @Override
        public <T> T invokeAny( Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit )
        {
            return null;
        }

        @Override
        public void execute( Runnable command )
        {

        }
    }
}
