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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.core.consensus.NoLeaderFoundException;
import org.neo4j.coreedge.core.consensus.roles.Role;
import org.neo4j.coreedge.core.state.machines.id.IdGenerationException;
import org.neo4j.coreedge.core.state.machines.locks.LeaderOnlyLockManager;
import org.neo4j.coreedge.edge.EdgeGraphDatabase;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.test.DbRepresentation;

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

    private Map<Integer, CoreClusterMember> coreMembers = new ConcurrentHashMap<>();
    private Map<Integer, EdgeClusterMember> edgeMembers = new ConcurrentHashMap<>();

    public Cluster( File parentDir, int noOfCoreMembers, int noOfEdgeMembers,
                    DiscoveryServiceFactory discoveryServiceFactory,
                    Map<String, String> coreParams, Map<String, IntFunction<String>> instanceCoreParams,
                    Map<String, String> edgeParams, Map<String, IntFunction<String>> instanceEdgeParams,
                    String recordFormat )
    {
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.parentDir = parentDir;
        List<AdvertisedSocketAddress> initialHosts = buildAddresses( noOfCoreMembers );
        createCoreMembers( noOfCoreMembers, initialHosts, coreParams, instanceCoreParams, recordFormat );
        createEdgeMembers( noOfEdgeMembers, initialHosts, edgeParams, instanceEdgeParams, recordFormat );
    }

    public void start() throws InterruptedException, ExecutionException
    {
        ExecutorService executor = Executors.newCachedThreadPool( new NamedThreadFactory( "cluster-starter" ) );
        try
        {
            CompletionService<EdgeGraphDatabase> edgeGraphDatabaseCompletionService = startEdgeMembers( executor );
            startCoreMembers( executor );
            waitForEdgeServers( edgeGraphDatabaseCompletionService );
        }
        finally
        {
            executor.shutdown();
        }
    }

    private void waitForEdgeServers( CompletionService<EdgeGraphDatabase> edgeGraphDatabaseCompletionService ) throws
            InterruptedException, ExecutionException
    {
        for ( int i = 0; i < edgeMembers.size(); i++ )
        {
            edgeGraphDatabaseCompletionService.take().get();
        }
    }

    public Set<CoreClusterMember> healthyCoreMembers()
    {
        return coreMembers.values().stream()
                .filter( db -> db.database().getDependencyResolver().resolveDependency( DatabaseHealth.class )
                        .isHealthy() )
                .collect( Collectors.toSet() );
    }

    public CoreClusterMember getCoreMemberById( int memberId )
    {
        return coreMembers.get( memberId );
    }

    public EdgeClusterMember getEdgeMemberById( int memberId )
    {
        return edgeMembers.get( memberId );
    }

    public CoreClusterMember addCoreMemberWithId( int memberId, int intendedClusterSize )
    {
        return addCoreMemberWithId( memberId, intendedClusterSize, stringMap(), emptyMap(), StandardV3_0.NAME );
    }

    public EdgeClusterMember addEdgeMemberWithIdAndRecordFormat( int memberId, String recordFormat )
    {
        CoreClusterMember coreClusterMember = coreMembers.values().stream().filter( ( member ) -> member.database()
                != null )
                .findAny().orElseThrow( () -> new IllegalStateException(
                        "No core members are running to use as a template for the edge member" ) );
        Config config = coreClusterMember.database().getDependencyResolver().resolveDependency( Config.class );

        List<AdvertisedSocketAddress> hazelcastAddresses =
                config.get( CoreEdgeClusterSettings.initial_discovery_members );
        EdgeClusterMember member = new EdgeClusterMember( parentDir, memberId, discoveryServiceFactory,
                hazelcastAddresses, stringMap(), emptyMap(), recordFormat );
        edgeMembers.put( memberId, member );
        return member;
    }

    public EdgeClusterMember addEdgeMemberWithId( int memberId )
    {
        return addEdgeMemberWithIdAndRecordFormat( memberId, StandardV3_0.NAME );
    }

    public void shutdown() throws ExecutionException, InterruptedException
    {
        shutdownCoreMembers();
        shutdownEdgeMembers();
    }

    private void shutdownCoreMembers() throws InterruptedException, ExecutionException
    {
        ExecutorService executor = Executors.newCachedThreadPool();
        List<Callable<Object>> memberShutdownSuppliers = new ArrayList<>();
        for ( final CoreClusterMember coreClusterMember : coreMembers.values() )
        {
            memberShutdownSuppliers.add( () ->
            {
                coreClusterMember.shutdown();
                return null;
            } );
        }

        try
        {
            combine( executor.invokeAll( memberShutdownSuppliers ) ).get();
        }
        finally
        {
            executor.shutdown();
        }
    }

    public void removeCoreMemberWithMemberId( int memberId )
    {
        CoreClusterMember memberToRemove = getCoreMemberById( memberId );

        if ( memberToRemove != null )
        {
            memberToRemove.shutdown();
            removeCoreMember( memberToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core meber with member id " + memberId );
        }
    }

    public void removeCoreMember( CoreClusterMember memberToRemove )
    {
        memberToRemove.shutdown();
        coreMembers.values().remove( memberToRemove );
    }

    public void removeEdgeMemberWithMemberId( int memberId )
    {
        EdgeClusterMember memberToRemove = getEdgeMemberById( memberId );

        if ( memberToRemove != null )
        {
            removeEdgeMember( memberToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core member with member id " + memberId );
        }
    }

    private void removeEdgeMember( EdgeClusterMember memberToRemove )
    {
        memberToRemove.shutdown();
        edgeMembers.values().remove( memberToRemove );
    }

    public Collection<CoreClusterMember> coreMembers()
    {
        return coreMembers.values();
    }

    public Collection<EdgeClusterMember> edgeMembers()
    {
        return edgeMembers.values();
    }

    public EdgeClusterMember findAnEdgeMember()
    {
        return firstOrNull( edgeMembers.values() );
    }

    public CoreClusterMember getDbWithRole( Role role )
    {
        for ( CoreClusterMember coreClusterMember : coreMembers.values() )
        {
            if ( coreClusterMember.database() != null && coreClusterMember.database().getRole().equals( role ) )
            {
                return coreClusterMember;
            }
        }
        return null;
    }

    public CoreClusterMember awaitLeader() throws TimeoutException
    {
        return awaitCoreMemberWithRole( DEFAULT_TIMEOUT_MS, Role.LEADER );
    }

    public CoreClusterMember awaitLeader( long timeoutMillis ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( timeoutMillis, Role.LEADER );
    }

    public CoreClusterMember awaitCoreMemberWithRole( long timeoutMillis, Role role ) throws TimeoutException
    {
        long endTimeMillis = timeoutMillis + System.currentTimeMillis();

        CoreClusterMember db;
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

    public int numberOfCoreMembersReportedByTopology()
    {
        CoreClusterMember aCoreGraphDb = coreMembers.values().stream()
                .filter( ( member ) -> member.database() != null ).findAny().get();
        CoreTopologyService coreTopologyService = aCoreGraphDb.database().getDependencyResolver()
                .resolveDependency( CoreTopologyService.class );
        return coreTopologyService.currentTopology().coreMembers().size();
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreClusterMember coreTx( BiConsumer<CoreGraphDatabase, Transaction> op )
            throws TimeoutException, InterruptedException
    {
        // this currently wraps the leader-only strategy, since it is the recommended and only approach
        return leaderTx( op );
    }

    private CoreClusterMember addCoreMemberWithId( int memberId, int intendedClusterSize, Map<String, String>
            extraParams,
                                                   Map<String, IntFunction<String>> instanceExtraParams, String
                                                           recordFormat )
    {
        Config config = firstOrNull( coreMembers.values() ).database().getDependencyResolver().resolveDependency(
                Config.class );
        List<AdvertisedSocketAddress> advertisedAddress = config.get( CoreEdgeClusterSettings
                .initial_discovery_members );

        CoreClusterMember coreClusterMember = new CoreClusterMember( memberId, intendedClusterSize, advertisedAddress,
                discoveryServiceFactory, recordFormat, parentDir,
                extraParams, instanceExtraParams );
        coreMembers.put( memberId, coreClusterMember );
        return coreClusterMember;
    }

    /**
     * Perform a transaction against the leader of the core cluster, retrying as necessary.
     */
    private CoreClusterMember leaderTx( BiConsumer<CoreGraphDatabase, Transaction> op )
            throws TimeoutException, InterruptedException
    {
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            CoreClusterMember member = awaitCoreMemberWithRole( DEFAULT_TIMEOUT_MS, Role.LEADER );
            CoreGraphDatabase db = member.database();

            try
            {
                Transaction tx = db.beginTx();
                op.accept( db, tx );
                tx.close();
                return member;
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
                (e.getMessage().equals( LeaderOnlyLockManager.LOCK_NOT_ON_LEADER_ERROR_MESSAGE ) ||
                        e.getCause() instanceof NoLeaderFoundException);
    }

    private boolean isLockExpired( Throwable e )
    {
        return e instanceof TransactionFailureException &&
                e.getCause() instanceof org.neo4j.kernel.api.exceptions.TransactionFailureException &&
                ((org.neo4j.kernel.api.exceptions.TransactionFailureException) e.getCause()).status() ==
                        LockSessionExpired;
    }

    private static List<AdvertisedSocketAddress> buildAddresses( int noOfCoreMembers )
    {
        List<AdvertisedSocketAddress> addresses = new ArrayList<>();
        for ( int i = 0; i < noOfCoreMembers; i++ )
        {
            int port = 5000 + i;
            addresses.add( new AdvertisedSocketAddress( "localhost:" + port ) );
        }
        return addresses;
    }

    private void createCoreMembers( final int noOfCoreMembers,
                                    List<AdvertisedSocketAddress> addresses, Map<String, String> extraParams,
                                    Map<String, IntFunction<String>> instanceExtraParams, String recordFormat )
    {

        for ( int i = 0; i < noOfCoreMembers; i++ )
        {
            CoreClusterMember coreClusterMember = new CoreClusterMember( i, noOfCoreMembers, addresses,
                    discoveryServiceFactory, recordFormat, parentDir, extraParams, instanceExtraParams );
            coreMembers.put( i, coreClusterMember );
        }
    }

    private void startCoreMembers( ExecutorService executor ) throws InterruptedException, ExecutionException
    {
        CompletionService<CoreGraphDatabase> ecs = new ExecutorCompletionService<>( executor );

        for ( CoreClusterMember coreClusterMember : coreMembers.values() )
        {
            ecs.submit( () ->
            {
                coreClusterMember.start();
                return coreClusterMember.database();
            } );
        }

        for ( int i = 0; i < coreMembers.size(); i++ )
        {
            ecs.take().get();
        }
    }

    private CompletionService<EdgeGraphDatabase> startEdgeMembers( ExecutorService executor ) throws InterruptedException, ExecutionException
    {
        CompletionService<EdgeGraphDatabase> ecs = new ExecutorCompletionService<>( executor );

        for ( EdgeClusterMember edgeClusterMember : edgeMembers.values() )
        {
            ecs.submit( () ->
            {
                edgeClusterMember.start();
                return edgeClusterMember.database();
            } );
        }
        return ecs;
    }

    private void createEdgeMembers( int noOfEdgeMembers,
                                    final List<AdvertisedSocketAddress> coreMemberAddresses,
                                    Map<String, String> extraParams,
                                    Map<String, IntFunction<String>> instanceExtraParams,
                                    String recordFormat )
    {

        for ( int i = 0; i < noOfEdgeMembers; i++ )
        {
            edgeMembers.put( i, new EdgeClusterMember( parentDir, i, discoveryServiceFactory, coreMemberAddresses,
                    extraParams, instanceExtraParams, recordFormat ) );
        }
    }

    private void shutdownEdgeMembers()
    {
        edgeMembers.values().forEach( EdgeClusterMember::shutdown );
    }

    public static void dataMatchesEventually( CoreClusterMember member, Collection<CoreClusterMember> targetDBs )
            throws TimeoutException, InterruptedException
    {
        CoreGraphDatabase sourceDB = member.database();
        DbRepresentation sourceRepresentation = DbRepresentation.of( sourceDB );
        for ( CoreClusterMember targetDB : targetDBs )
        {
            Predicates.await( () -> sourceRepresentation.equals( DbRepresentation.of( targetDB.database() ) ),
                    DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }
}
