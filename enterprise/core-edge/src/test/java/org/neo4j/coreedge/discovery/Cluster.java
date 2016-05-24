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

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;

import org.neo4j.coreedge.raft.NoLeaderFoundException;
import org.neo4j.coreedge.raft.replication.id.IdGenerationException;
import org.neo4j.coreedge.raft.roles.Role;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.coreedge.server.core.locks.LeaderOnlyLockManager;
import org.neo4j.coreedge.server.edge.EdgeGraphDatabase;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.joining;
import static org.neo4j.concurrent.Futures.combine;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

public class Cluster implements AutoCloseable
{
    private static final String CLUSTER_NAME = "core-neo4j";
    private static final int DEFAULT_TIMEOUT_MS = 15_000;
    private static final int DEFAULT_BACKOFF_MS = 100;

    private final File parentDir;
    private final Map<String,String> coreParams;
    private Map<String,IntFunction<String>> instanceCoreParams;
    private final Map<String, String> edgeParams;
    private final Map<String, IntFunction<String>> instanceEdgeParams;
    private final String recordFormat;
    private final DiscoveryServiceFactory discoveryServiceFactory;
    private final int noOfCoreServers;
    private final int noOfEdgeServers;

    private Set<CoreGraphDatabase> coreServers = new HashSet<>();
    private Set<EdgeGraphDatabase> edgeServers = new HashSet<>();

    public Cluster( File parentDir, int noOfCoreServers, int noOfEdgeServers, DiscoveryServiceFactory fatory,
            Map<String,String> coreParams, Map<String,IntFunction<String>> instanceCoreParams,
                    Map<String,String> edgeParams, Map<String,IntFunction<String>> instanceEdgeParams, String recordFormat )
            throws ExecutionException, InterruptedException
    {
        this.noOfCoreServers = noOfCoreServers;
        this.noOfEdgeServers = noOfEdgeServers;
        this.discoveryServiceFactory = fatory;
        this.parentDir = parentDir;
        this.coreParams = coreParams;
        this.instanceCoreParams = instanceCoreParams;
        this.edgeParams = edgeParams;
        this.instanceEdgeParams = instanceEdgeParams;
        this.recordFormat = recordFormat;
    }

    public void start() throws InterruptedException, ExecutionException
    {
        List<AdvertisedSocketAddress> initialHosts = buildAddresses( noOfCoreServers );
        ExecutorService executor = Executors.newCachedThreadPool();
        try
        {
            startCoreServers( executor, noOfCoreServers, initialHosts, coreParams, instanceCoreParams, recordFormat );
            startEdgeServers( executor, noOfEdgeServers, initialHosts, edgeParams, instanceEdgeParams, recordFormat );
        }
        finally
        {
            executor.shutdown();
        }
    }

    public static Cluster start( File parentDir, int noOfCoreServers, int noOfEdgeServers,
            DiscoveryServiceFactory discoveryServiceFactory, Map<String,String> coreParams,
            Map<String, IntFunction<String>> instanceCoreParams,  String recordFormat )
            throws ExecutionException, InterruptedException
    {
        Cluster cluster = new Cluster( parentDir, noOfCoreServers, noOfEdgeServers, discoveryServiceFactory,
                coreParams, instanceCoreParams, stringMap(), emptyMap(), recordFormat );
        cluster.start();
        return cluster;
    }

    public static Cluster start( File parentDir, int noOfCoreServers, int noOfEdgeServers,
            DiscoveryServiceFactory discoveryServiceFactory, String recordFormat )
            throws ExecutionException, InterruptedException
    {
        return start( parentDir, noOfCoreServers, noOfEdgeServers, discoveryServiceFactory, stringMap(), emptyMap(),
                recordFormat );
    }

    public static Cluster start( File parentDir, int noOfCoreServers, int noOfEdgeServers,
            DiscoveryServiceFactory discoveryServiceFactory ) throws ExecutionException, InterruptedException
    {
        return start( parentDir, noOfCoreServers, noOfEdgeServers, discoveryServiceFactory, stringMap(), emptyMap(),
                StandardV3_0.NAME );
    }

    public static Cluster start( File parentDir, int noOfCoreServers, int noOfEdgeServers )
            throws ExecutionException, InterruptedException
    {
        return start( parentDir, noOfCoreServers, noOfEdgeServers, new HazelcastDiscoveryServiceFactory(), stringMap(),
                emptyMap(), StandardV3_0.NAME );
    }

    public static Cluster start( File parentDir, int noOfCoreServers, int noOfEdgeServers,
            Map<String,String> coreParams ) throws ExecutionException, InterruptedException
    {
        return start( parentDir, noOfCoreServers, noOfEdgeServers, new HazelcastDiscoveryServiceFactory(), coreParams,
                emptyMap(), StandardV3_0.NAME );
    }

    public static Cluster start( File parentDir, int noOfCoreServers, int noOfEdgeServers,
            Map<String,String> coreParams, DiscoveryServiceFactory discoveryServiceFactory )
            throws ExecutionException, InterruptedException
    {
        return start( parentDir, noOfCoreServers, noOfEdgeServers, discoveryServiceFactory, coreParams, emptyMap(),
                StandardV3_0.NAME );
    }

    public File coreServerStoreDirectory( int serverId )
    {
        return coreServerStoreDirectory( parentDir, serverId );
    }

    private static File coreServerStoreDirectory( File parentDir, int serverId )
    {
        return new File( parentDir, "server-core-" + serverId );
    }

    public static File edgeServerStoreDirectory( File parentDir, int serverId )
    {
        return new File( parentDir, "server-edge-" + serverId );
    }

    private Map<String,String> serverParams( String serverType, int serverId, String initialHosts )
    {
        Map<String,String> params = stringMap();
        params.put( "dbms.mode", serverType );
        params.put( CoreEdgeClusterSettings.cluster_name.name(), CLUSTER_NAME );
        params.put( CoreEdgeClusterSettings.server_id.name(), String.valueOf( serverId ) );
        params.put( CoreEdgeClusterSettings.initial_core_cluster_members.name(), initialHosts );
        return params;
    }

    private static List<AdvertisedSocketAddress> buildAddresses( int noOfCoreServers )
    {
        List<AdvertisedSocketAddress> addresses = new ArrayList<>();
        for ( int i = 0; i < noOfCoreServers; i++ )
        {
            int port = 5000 + i;
            addresses.add( new AdvertisedSocketAddress( "localhost:" + port ) );
        }
        return addresses;
    }

    private void startCoreServers( ExecutorService executor, final int noOfCoreServers,
            List<AdvertisedSocketAddress> addresses, Map<String,String> extraParams,
            Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
            throws InterruptedException, ExecutionException
    {
        CompletionService<CoreGraphDatabase> ecs = new ExecutorCompletionService<>( executor );

        for ( int i = 0; i < noOfCoreServers; i++ )
        {
            final int serverId = i;
            ecs.submit( () -> startCoreServer( serverId, noOfCoreServers, addresses, extraParams,
                    instanceExtraParams, recordFormat ) );
        }

        for ( int i = 0; i < noOfCoreServers; i++ )
        {
            this.coreServers.add( ecs.take().get() );
        }
    }

    private void startEdgeServers( ExecutorService executor, int noOfEdgeServers,
            final List<AdvertisedSocketAddress> addresses,
                                   Map<String,String> extraParams, Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
            throws InterruptedException, ExecutionException
    {
        CompletionService<EdgeGraphDatabase> ecs = new ExecutorCompletionService<>( executor );

        for ( int i = 0; i < noOfEdgeServers; i++ )
        {
            final int serverId = i;
            ecs.submit( () -> startEdgeServer( serverId, addresses, extraParams, instanceExtraParams, recordFormat ) );
        }

        for ( int i = 0; i < noOfEdgeServers; i++ )
        {
            this.edgeServers.add( ecs.take().get() );
        }
    }

    private CoreGraphDatabase startCoreServer( int serverId, int clusterSize, List<AdvertisedSocketAddress> addresses,
            Map<String,String> extraParams, Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        int clusterPort = 5000 + serverId;
        int txPort = 6000 + serverId;
        int raftPort = 7000 + serverId;
        int boltPort = 8000 + serverId;

        String initialHosts = addresses.stream().map( AdvertisedSocketAddress::toString ).collect( joining( "," ) );

        final Map<String,String> params = serverParams( "CORE", serverId, initialHosts );

        params.put( GraphDatabaseSettings.record_format.name(), recordFormat );

        params.put( CoreEdgeClusterSettings.cluster_listen_address.name(), "localhost:" + clusterPort );

        params.put( CoreEdgeClusterSettings.transaction_advertised_address.name(), "localhost:" + txPort );
        params.put( CoreEdgeClusterSettings.transaction_listen_address.name(), "127.0.0.1:" + txPort );
        params.put( CoreEdgeClusterSettings.raft_advertised_address.name(), "localhost:" + raftPort );
        params.put( CoreEdgeClusterSettings.raft_listen_address.name(), "127.0.0.1:" + raftPort );

        params.put( new GraphDatabaseSettings.BoltConnector("bolt").type.name(), "BOLT" );
        params.put( new GraphDatabaseSettings.BoltConnector("bolt").enabled.name(), "true" );
        params.put( new GraphDatabaseSettings.BoltConnector("bolt").address.name(), "0.0.0.0:" + boltPort );

        params.put( CoreEdgeClusterSettings.bolt_advertised_address.name(), "127.0.0.1:" + boltPort );

        params.put( CoreEdgeClusterSettings.expected_core_cluster_size.name(), String.valueOf( clusterSize ) );
        params.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        params.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );

        params.putAll( extraParams );

        for ( Map.Entry<String, IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            params.put( entry.getKey(), entry.getValue().apply( serverId ) );
        }

        final File storeDir = coreServerStoreDirectory( parentDir, serverId );
        params.put( GraphDatabaseSettings.logs_directory.name(), storeDir.getAbsolutePath() );
        return new CoreGraphDatabase( storeDir, params, GraphDatabaseDependencies.newDependencies(),
                discoveryServiceFactory );
    }

    private EdgeGraphDatabase startEdgeServer( int serverId, List<AdvertisedSocketAddress> addresses,
                                               Map<String,String> extraParams, Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        final File storeDir = edgeServerStoreDirectory( parentDir, serverId );
        return startEdgeServer( serverId, storeDir, addresses, extraParams, instanceExtraParams, recordFormat );
    }

    private EdgeGraphDatabase startEdgeServer( int serverId, File storeDir, List<AdvertisedSocketAddress> addresses,
                                               Map<String,String> extraParams, Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        String initialHosts = addresses.stream().map( AdvertisedSocketAddress::toString ).collect( joining( "," ) );

        final Map<String, String> params = serverParams( "EDGE", serverId, initialHosts );
        params.put(GraphDatabaseSettings.record_format.name(), recordFormat);
        params.put( GraphDatabaseSettings.pagecache_memory.name(), "8m" );
        params.put( GraphDatabaseSettings.auth_store.name(), new File( parentDir, "auth" ).getAbsolutePath() );
        params.put( GraphDatabaseSettings.logs_directory.name(), storeDir.getAbsolutePath() );

        params.putAll( extraParams );

        for ( Map.Entry<String, IntFunction<String>> entry : instanceExtraParams.entrySet() )
        {
            params.put( entry.getKey(), entry.getValue().apply( serverId ) );
        }
        params.put( new GraphDatabaseSettings.BoltConnector("bolt").type.name(), "BOLT" );
        params.put( new GraphDatabaseSettings.BoltConnector("bolt").enabled.name(), "true" );
        params.put( new GraphDatabaseSettings.BoltConnector("bolt").address.name(), "0.0.0.0:" + (9000 + serverId ));

        params.put( CoreEdgeClusterSettings.bolt_advertised_address.name(), "127.0.0.1:" + (9000 + serverId) );

        return new EdgeGraphDatabase( storeDir, params, GraphDatabaseDependencies.newDependencies(),
                discoveryServiceFactory );
    }

    public void shutdown() throws ExecutionException, InterruptedException
    {
        shutdownCoreServers();
        shutdownEdgeServers();
    }

    public void shutdownCoreServers() throws InterruptedException, ExecutionException
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Callable<Object>> serverShutdownSuppliers = new ArrayList<>();
        for ( final CoreGraphDatabase coreServer : coreServers )
        {
            serverShutdownSuppliers.add( () -> {
                coreServer.shutdown();
                return null;
            } );
        }

        try
        {
            combine( executor.invokeAll( serverShutdownSuppliers ) ).get();
        }
        finally
        {
            executor.shutdown();
            coreServers.clear();
        }
    }

    private void shutdownEdgeServers()
    {
        for ( EdgeGraphDatabase edgeServer : edgeServers )
        {
            edgeServer.shutdown();
        }
        edgeServers.clear();
    }

    public CoreGraphDatabase getCoreServerById( int serverId )
    {
        for ( CoreGraphDatabase coreServer : coreServers )
        {
            if ( serverIdFor( coreServer ) == serverId )
            {
                return coreServer;
            }
        }
        return null;
    }

    public EdgeGraphDatabase getEdgeServerById( int serverId )
    {
        for ( EdgeGraphDatabase edgeServer : edgeServers )
        {
            if ( serverIdFor( edgeServer ) == serverId )
            {
                return edgeServer;
            }
        }
        return null;
    }

    public void removeCoreServerWithServerId( int serverId )
    {
        CoreGraphDatabase serverToRemove = getCoreServerById( serverId );

        if ( serverToRemove != null )
        {
            removeCoreServer( serverToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core server with server id " + serverId );
        }
    }

    public void removeCoreServer( CoreGraphDatabase serverToRemove )
    {
        serverToRemove.shutdown();
        coreServers.remove( serverToRemove );
    }

    public void removeEdgeServerWithServerId( int serverId )
    {
        EdgeGraphDatabase serverToRemove = null;
        for ( EdgeGraphDatabase edgeServer : edgeServers )
        {
            if ( serverIdFor( edgeServer ) == serverId )
            {
                edgeServer.shutdown();
                serverToRemove = edgeServer;
            }
        }

        if ( serverToRemove == null )
        {
            throw new RuntimeException( "Could not remove edge server with server id " + serverId );
        }
        else
        {
            edgeServers.remove( serverToRemove );
        }
    }

    public void addCoreServerWithServerId( int serverId, int intendedClusterSize )
    {
        addCoreServerWithServerId( serverId, intendedClusterSize, stringMap(), emptyMap(), StandardV3_0.NAME );
    }

    private void addCoreServerWithServerId( int serverId, int intendedClusterSize, Map<String,String> extraParams,
            Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        Config config = firstOrNull( coreServers ).getDependencyResolver().resolveDependency( Config.class );
        List<AdvertisedSocketAddress> advertisedAddress =
                config.get( CoreEdgeClusterSettings.initial_core_cluster_members );

        coreServers.add( startCoreServer( serverId, intendedClusterSize, advertisedAddress, extraParams,
                instanceExtraParams, recordFormat ) );
    }

    public void addEdgeServerWithFileLocation( int serverId, String recordFormat )
    {
        Config config = coreServers.iterator().next().getDependencyResolver().resolveDependency( Config.class );
        List<AdvertisedSocketAddress> advertisedAddresses =
                config.get( CoreEdgeClusterSettings.initial_core_cluster_members );

        edgeServers.add( startEdgeServer( serverId, advertisedAddresses, stringMap(), emptyMap(), recordFormat ) );
    }

    public void addEdgeServerWithFileLocation( int serverId )
    {
        addEdgeServerWithFileLocation( serverId, StandardV3_0.NAME );
    }

    private int serverIdFor( GraphDatabaseFacade graphDatabaseFacade )
    {
        return graphDatabaseFacade.getDependencyResolver().resolveDependency( Config.class )
                .get( CoreEdgeClusterSettings.server_id );
    }

    public Set<CoreGraphDatabase> coreServers()
    {
        return coreServers;
    }

    public Set<EdgeGraphDatabase> edgeServers()
    {
        return edgeServers;
    }

    public EdgeGraphDatabase findAnEdgeServer()
    {
        return edgeServers.iterator().next();
    }

    public CoreGraphDatabase getDbWithRole( Role role )
    {
        for ( CoreGraphDatabase coreServer : coreServers )
        {
            if ( coreServer.getRole().equals( role ) )
            {
                return coreServer;
            }
        }
        return null;
    }

    public CoreGraphDatabase awaitLeader() throws TimeoutException
    {
        return awaitCoreGraphDatabaseWithRole( DEFAULT_TIMEOUT_MS, Role.LEADER );
    }

    public CoreGraphDatabase awaitLeader( long timeoutMillis ) throws TimeoutException
    {
        return awaitCoreGraphDatabaseWithRole( timeoutMillis, Role.LEADER );
    }

    public CoreGraphDatabase awaitCoreGraphDatabaseWithRole( long timeoutMillis, Role role ) throws TimeoutException
    {
        long endTimeMillis = timeoutMillis + System.currentTimeMillis();

        CoreGraphDatabase db;
        while ( (db = getDbWithRole( role )) == null && (System.currentTimeMillis() < endTimeMillis) )
        {
            LockSupport.parkNanos( MILLISECONDS.toNanos( 100 ) );
        }

        if ( db == null )
        {
            throw new TimeoutException();
        }
        return db;
    }

    public int numberOfCoreServers()
    {
        CoreGraphDatabase aCoreGraphDb = coreServers.iterator().next();
        CoreTopologyService coreTopologyService = aCoreGraphDb.getDependencyResolver()
                .resolveDependency( CoreTopologyService.class );
        return coreTopologyService.currentTopology().coreMembers().size();
    }

    public void addEdgeServerWithFileLocation( File edgeDatabaseStoreFileLocation )
    {
        Config config = coreServers.iterator().next().getDependencyResolver().resolveDependency( Config.class );
        List<AdvertisedSocketAddress> advertisedAddresses =
                config.get( CoreEdgeClusterSettings.initial_core_cluster_members );

        edgeServers.add( startEdgeServer( 999, edgeDatabaseStoreFileLocation, advertisedAddresses,
                stringMap(), emptyMap(), StandardV3_0.NAME ) );
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreGraphDatabase coreTx( BiConsumer<CoreGraphDatabase,Transaction> op )
            throws TimeoutException, InterruptedException
    {
        // this currently wraps the leader-only strategy, since it is the recommended and only approach
        return leaderTx( op );
    }

    /**
     * Perform a transaction against the leader of the core cluster, retrying as necessary.
     */
    private CoreGraphDatabase leaderTx( BiConsumer<CoreGraphDatabase,Transaction> op )
            throws TimeoutException, InterruptedException
    {
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            CoreGraphDatabase db = awaitCoreGraphDatabaseWithRole( DEFAULT_TIMEOUT_MS, Role.LEADER );

            try
            {
                Transaction tx = db.beginTx();
                op.accept( db, tx );
                tx.close();
                return db;
            }
            catch ( Throwable e )
            {
                if ( isTransientFailure( e ) )
                {
                    // sleep and retry
                    Thread.sleep( DEFAULT_BACKOFF_MS );
                }
                else
                {
                    throw e;
                }
            }
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }

    private boolean isTransientFailure( Throwable e )
    {
        // TODO: This should really catch all cases of transient failures. Must be able to express that in a clearer manner...
        return ( e instanceof IdGenerationException ) || isLockExpired( e ) || isLockOnFollower( e );

    }

    private boolean isLockOnFollower( Throwable e )
    {
        return e instanceof AcquireLockTimeoutException && ( e.getMessage().equals( LeaderOnlyLockManager.LOCK_NOT_ON_LEADER_ERROR_MESSAGE ) || e.getCause() instanceof NoLeaderFoundException);
    }

    private boolean isLockExpired( Throwable e )
    {
        return e instanceof TransactionFailureException &&
                e.getCause() instanceof org.neo4j.kernel.api.exceptions.TransactionFailureException &&
                ((org.neo4j.kernel.api.exceptions.TransactionFailureException) e.getCause()).status() ==
                        LockSessionExpired;
    }

    @Override
    public void close() throws Exception
    {
        shutdown();
    }
}
