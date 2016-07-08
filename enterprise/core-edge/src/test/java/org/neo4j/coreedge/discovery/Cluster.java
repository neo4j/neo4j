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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

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
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.concurrent.Futures.combine;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

public class Cluster
{
    private static final int DEFAULT_TIMEOUT_MS = 15_000;
    private static final int DEFAULT_BACKOFF_MS = 100;

    private final File parentDir;
    private final DiscoveryServiceFactory discoveryServiceFactory;

    private Map<Integer, CoreServer> coreServers = new ConcurrentHashMap<>();
    private Map<Integer, EdgeServer> edgeServers = new ConcurrentHashMap<>();

    public Cluster( File parentDir, int noOfCoreServers, int noOfEdgeServers,
                    DiscoveryServiceFactory discoveryServiceFactory,
                    Map<String, String> coreParams, Map<String, IntFunction<String>> instanceCoreParams,
                    Map<String, String> edgeParams, Map<String, IntFunction<String>> instanceEdgeParams,
                    String recordFormat )
    {
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.parentDir = parentDir;
        List<AdvertisedSocketAddress> initialHosts = buildAddresses( noOfCoreServers );
        createCoreServers( noOfCoreServers, initialHosts, coreParams, instanceCoreParams, recordFormat );
        createEdgeServers( noOfEdgeServers, initialHosts, edgeParams, instanceEdgeParams, recordFormat );
    }

    public void start() throws InterruptedException, ExecutionException
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        try
        {
            startCoreServers( executor );
            startEdgeServers( executor );
        }
        finally
        {
            executor.shutdown();
        }
    }

    public Set<CoreServer> healthyCoreMembers()
    {
        return coreServers.values().stream()
                .filter( db -> db.database().getDependencyResolver().resolveDependency( DatabaseHealth.class ).isHealthy() )
                .collect( Collectors.toSet() );
    }

    public CoreServer getCoreServerById( int serverId )
    {
        return coreServers.get( serverId );
    }

    public EdgeServer getEdgeServerById( int serverId )
    {
        return edgeServers.get( serverId );
    }

    public CoreServer addCoreServerWithServerId( int serverId, int intendedClusterSize )
    {
        return addCoreServerWithServerId( serverId, intendedClusterSize, stringMap(), emptyMap(), StandardV3_0.NAME );
    }

    public EdgeServer addEdgeServerWithIdAndRecordFormat( int serverId, String recordFormat )
    {
        CoreServer coreServer = coreServers.values().stream().filter( ( server ) -> server.database() != null )
                .findAny().orElseThrow( () -> new IllegalStateException(
                        "No core servers are running to use as a template for the edge server" ) );
        Config config = coreServer.database().getDependencyResolver().resolveDependency( Config.class );

        List<AdvertisedSocketAddress> advertisedAddresses =
                config.get( CoreEdgeClusterSettings.initial_core_cluster_members );

        EdgeServer server = new EdgeServer( parentDir, serverId, discoveryServiceFactory, advertisedAddresses,
                stringMap(), emptyMap(), recordFormat );
        edgeServers.put( serverId, server );
        return server;
    }

    public EdgeServer addEdgeServerWithId( int serverId )
    {
        return addEdgeServerWithIdAndRecordFormat( serverId, StandardV3_0.NAME );
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
        for ( final CoreServer coreServer : coreServers.values() )
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
        }
    }

    public void removeCoreServerWithServerId( int serverId )
    {
        CoreServer serverToRemove = getCoreServerById( serverId );

        if ( serverToRemove != null )
        {
            serverToRemove.shutdown();
            removeCoreServer( serverToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core server with server id " + serverId );
        }
    }

    public void removeCoreServer( CoreServer serverToRemove )
    {
        serverToRemove.shutdown();
        coreServers.values().remove( serverToRemove );
    }

    public void removeEdgeServerWithServerId( int serverId )
    {
        EdgeServer serverToRemove = getEdgeServerById( serverId );

        if ( serverToRemove != null )
        {
            removeEdgeServer( serverToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core server with server id " + serverId );
        }
    }

    public void removeEdgeServer( EdgeServer serverToRemove )
    {
        serverToRemove.shutdown();
        edgeServers.values().remove( serverToRemove );
    }

    public Collection<CoreServer> coreServers()
    {
        return coreServers.values();
    }

    public Collection<EdgeServer> edgeServers()
    {
        return edgeServers.values();
    }

    public EdgeServer findAnEdgeServer()
    {
        return firstOrNull( edgeServers.values() );
    }

    public CoreServer getDbWithRole( Role role )
    {
        for ( CoreServer coreServer : coreServers.values() )
        {
            if ( coreServer.database() != null && coreServer.database().getRole().equals( role ) )
            {
                return coreServer;
            }
        }
        return null;
    }

    public CoreServer awaitLeader() throws TimeoutException
    {
        return awaitCoreGraphDatabaseWithRole( DEFAULT_TIMEOUT_MS, Role.LEADER );
    }

    public CoreServer awaitLeader( long timeoutMillis ) throws TimeoutException
    {
        return awaitCoreGraphDatabaseWithRole( timeoutMillis, Role.LEADER );
    }

    public CoreServer awaitCoreGraphDatabaseWithRole( long timeoutMillis, Role role ) throws TimeoutException
    {
        long endTimeMillis = timeoutMillis + System.currentTimeMillis();

        CoreServer db;
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
        CoreServer aCoreGraphDb = coreServers.values().stream()
                .filter( ( server ) -> server.database() != null ).findAny().get();
        CoreTopologyService coreTopologyService = aCoreGraphDb.database().getDependencyResolver()
                .resolveDependency( CoreTopologyService.class );
        return coreTopologyService.currentTopology().coreMembers().size();
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreServer coreTx( BiConsumer<CoreGraphDatabase, Transaction> op )
            throws TimeoutException, InterruptedException
    {
        // this currently wraps the leader-only strategy, since it is the recommended and only approach
        return leaderTx( op );
    }

    private CoreServer addCoreServerWithServerId( int serverId, int intendedClusterSize, Map<String, String> extraParams,
                                                  Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        Config config = firstOrNull( coreServers.values() ).database().getDependencyResolver().resolveDependency( Config.class );
        List<AdvertisedSocketAddress> advertisedAddress = config.get( CoreEdgeClusterSettings.initial_core_cluster_members );

        CoreServer coreServer = new CoreServer( serverId, intendedClusterSize, advertisedAddress,
                discoveryServiceFactory, recordFormat, parentDir,
                extraParams, instanceExtraParams );
        coreServers.put( serverId, coreServer );
        return coreServer;
    }

    /**
     * Perform a transaction against the leader of the core cluster, retrying as necessary.
     */
    private CoreServer leaderTx( BiConsumer<CoreGraphDatabase, Transaction> op )
            throws TimeoutException, InterruptedException
    {
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            CoreServer server = awaitCoreGraphDatabaseWithRole( DEFAULT_TIMEOUT_MS, Role.LEADER );
            CoreGraphDatabase db = server.database();

            try
            {
                Transaction tx = db.beginTx();
                op.accept( db, tx );
                tx.close();
                return server;
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
        // TODO: This should really catch all cases of transient failures. Must be able to express that in a clearer
        // manner...
        return (e instanceof IdGenerationException) || isLockExpired( e ) || isLockOnFollower( e );

    }

    private boolean isLockOnFollower( Throwable e )
    {
        return e instanceof AcquireLockTimeoutException &&
                (e.getMessage().equals( LeaderOnlyLockManager .LOCK_NOT_ON_LEADER_ERROR_MESSAGE ) ||
                e.getCause() instanceof NoLeaderFoundException);
    }

    private boolean isLockExpired( Throwable e )
    {
        return e instanceof TransactionFailureException &&
                e.getCause() instanceof org.neo4j.kernel.api.exceptions.TransactionFailureException &&
                ((org.neo4j.kernel.api.exceptions.TransactionFailureException) e.getCause()).status() ==
                        LockSessionExpired;
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

    private void createCoreServers( final int noOfCoreServers,
                                    List<AdvertisedSocketAddress> addresses, Map<String, String> extraParams,
                                    Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
    {

        for ( int i = 0; i < noOfCoreServers; i++ )
        {
            CoreServer coreServer = new CoreServer( i, noOfCoreServers, addresses, discoveryServiceFactory,
                    recordFormat, parentDir,
                    extraParams, instanceExtraParams );
            coreServers.put( i, coreServer );
        }
    }

    private void startCoreServers( ExecutorService executor ) throws InterruptedException, ExecutionException
    {
        CompletionService<CoreGraphDatabase> ecs = new ExecutorCompletionService<>( executor );

        for ( CoreServer coreServer : coreServers.values() )
        {
            ecs.submit( () -> {
                coreServer.start();
                return coreServer.database();
            } );
        }

        for ( int i = 0; i < coreServers.size(); i++ )
        {
            ecs.take().get();
        }
    }

    private void startEdgeServers( ExecutorService executor ) throws InterruptedException, ExecutionException
    {
        CompletionService<EdgeGraphDatabase> ecs = new ExecutorCompletionService<>( executor );

        for ( EdgeServer edgeServer : edgeServers.values() )
        {
            ecs.submit( () -> {
                edgeServer.start();
                return edgeServer.database();
            } );
        }

        for ( int i = 0; i < edgeServers.size(); i++ )
        {
            ecs.take().get();
        }
    }

    private void createEdgeServers( int noOfEdgeServers,
                                    final List<AdvertisedSocketAddress> addresses,
                                    Map<String, String> extraParams,
                                    Map<String, IntFunction<String>> instanceExtraParams,
                                    String recordFormat )
    {

        for ( int i = 0; i < noOfEdgeServers; i++ )
        {
            edgeServers.put( i, new EdgeServer( parentDir, i, discoveryServiceFactory, addresses,
                    extraParams, instanceExtraParams, recordFormat ) );
        }
    }

    private void shutdownEdgeServers()
    {
        edgeServers.values().forEach( EdgeServer::shutdown );
    }

}
