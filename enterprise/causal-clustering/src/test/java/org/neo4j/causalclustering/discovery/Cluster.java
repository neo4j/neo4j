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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
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
import java.util.function.BiConsumer;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.LeaderCanWrite;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.state.machines.id.IdGenerationException;
import org.neo4j.causalclustering.core.state.machines.locks.LeaderOnlyLockManager;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.test.DbRepresentation;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.concurrent.Futures.combine;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.function.Predicates.notNull;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

public class Cluster
{
    private static final int DEFAULT_TIMEOUT_MS = 25_000;
    private static final int DEFAULT_CLUSTER_SIZE = 3;

    private final File parentDir;
    private final DiscoveryServiceFactory discoveryServiceFactory;

    private Map<Integer, CoreClusterMember> coreMembers = new ConcurrentHashMap<>();
    private Map<Integer,ReadReplica> readReplicas = new ConcurrentHashMap<>();

    public Cluster( File parentDir, int noOfCoreMembers, int noOfReadReplicas,
                    DiscoveryServiceFactory discoveryServiceFactory,
                    Map<String, String> coreParams, Map<String, IntFunction<String>> instanceCoreParams,
                    Map<String, String> readReplicaParams, Map<String, IntFunction<String>> instanceReadReplicaParams,
                    String recordFormat )
    {
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.parentDir = parentDir;
        HashSet<Integer> coreServerIds = new HashSet<>();
        for ( int i = 0; i < noOfCoreMembers; i++ )
        {
            coreServerIds.add( i );
        }
        List<AdvertisedSocketAddress> initialHosts = buildAddresses( coreServerIds );
        createCoreMembers( noOfCoreMembers, initialHosts, coreParams, instanceCoreParams, recordFormat );
        createReadReplicas( noOfReadReplicas, initialHosts, readReplicaParams, instanceReadReplicaParams, recordFormat );
    }

    public void start() throws InterruptedException, ExecutionException
    {
        ExecutorService executor = Executors.newCachedThreadPool( new NamedThreadFactory( "cluster-starter" ) );
        try
        {
            startCoreMembers( executor );
            startReadReplicas( executor );
        }
        finally
        {
            executor.shutdown();
        }
    }

    private void waitForReadReplicas( CompletionService<ReadReplicaGraphDatabase> readReplicaGraphDatabaseCompletionService ) throws
            InterruptedException, ExecutionException
    {
        for ( int i = 0; i < readReplicas.size(); i++ )
        {
            readReplicaGraphDatabaseCompletionService.take().get();
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

    public ReadReplica getReadReplicaById( int memberId )
    {
        return readReplicas.get( memberId );
    }

    public CoreClusterMember addCoreMemberWithId( int memberId )
    {
        return addCoreMemberWithId( memberId, stringMap(), emptyMap(), Standard.LATEST_NAME );
    }

    public CoreClusterMember addCoreMemberWithIdAndInitialMembers( int memberId,
            List<AdvertisedSocketAddress> initialMembers )
    {
        CoreClusterMember coreClusterMember = new  CoreClusterMember( memberId, DEFAULT_CLUSTER_SIZE, initialMembers,
                discoveryServiceFactory, Standard.LATEST_NAME, parentDir,
                emptyMap(), emptyMap() );
        coreMembers.put( memberId, coreClusterMember );
        return coreClusterMember;
    }

    public ReadReplica addReadReplicaWithIdAndRecordFormat( int memberId, String recordFormat )
    {
        List<AdvertisedSocketAddress> hazelcastAddresses = buildAddresses( coreMembers.keySet() );
        ReadReplica member = new ReadReplica( parentDir, memberId, discoveryServiceFactory,
                hazelcastAddresses, stringMap(), emptyMap(), recordFormat );
        readReplicas.put( memberId, member );
        return member;
    }

    public ReadReplica addReadReplicaWithId( int memberId )
    {
        return addReadReplicaWithIdAndRecordFormat( memberId, Standard.LATEST_NAME );
    }

    public ReadReplica addReadReplicaWithIdAndMonitors( int memberId, Monitors monitors )
    {
        List<AdvertisedSocketAddress> hazelcastAddresses = buildAddresses( coreMembers.keySet() );
        ReadReplica member = new ReadReplica( parentDir, memberId, discoveryServiceFactory,
                hazelcastAddresses, stringMap(), emptyMap(), Standard.LATEST_NAME, monitors );
        readReplicas.put( memberId, member );
        return member;
    }

    public void shutdown() throws ExecutionException, InterruptedException
    {
        shutdownCoreMembers();
        shutdownReadReplicas();
    }

    public void shutdownCoreMembers() throws InterruptedException, ExecutionException
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

    public void removeReadReplicaWithMemberId( int memberId )
    {
        ReadReplica memberToRemove = getReadReplicaById( memberId );

        if ( memberToRemove != null )
        {
            removeReadReplica( memberToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core member with member id " + memberId );
        }
    }

    private void removeReadReplica( ReadReplica memberToRemove )
    {
        memberToRemove.shutdown();
        readReplicas.values().remove( memberToRemove );
    }

    public Collection<CoreClusterMember> coreMembers()
    {
        return coreMembers.values();
    }

    public Collection<ReadReplica> readReplicas()
    {
        return readReplicas.values();
    }

    public ReadReplica findAnyReadReplica()
    {
        return firstOrNull( readReplicas.values() );
    }

    public CoreClusterMember getDbWithRole( Role role )
    {
        return getDbWithAnyRole( role );
    }

    public CoreClusterMember getDbWithAnyRole( Role... roles )
    {
        Set<Role> roleSet = Arrays.stream( roles ).collect( toSet() );
        for ( CoreClusterMember coreClusterMember : coreMembers.values() )
        {
            if ( coreClusterMember.database() != null && roleSet.contains( coreClusterMember.database().getRole() ) )
            {
                return coreClusterMember;
            }
        }
        return null;
    }

    public CoreClusterMember awaitLeader() throws TimeoutException
    {
        return awaitCoreMemberWithRole( Role.LEADER, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    public CoreClusterMember awaitLeader( long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( Role.LEADER, timeout, timeUnit );
    }

    public CoreClusterMember awaitCoreMemberWithRole( Role role, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return await( () -> getDbWithRole( role ), notNull(), timeout, timeUnit );
    }

    public int numberOfCoreMembersReportedByTopology()
    {
        CoreClusterMember aCoreGraphDb = coreMembers.values().stream()
                .filter( ( member ) -> member.database() != null ).findAny().get();
        CoreTopologyService coreTopologyService = aCoreGraphDb.database().getDependencyResolver()
                .resolveDependency( CoreTopologyService.class );
        return coreTopologyService.coreServers().members().size();
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreClusterMember coreTx( BiConsumer<CoreGraphDatabase, Transaction> op ) throws Exception
    {
        // this currently wraps the leader-only strategy, since it is the recommended and only approach
        return leaderTx( op, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    private CoreClusterMember addCoreMemberWithId( int memberId, Map<String,String> extraParams, Map<String,IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        List<AdvertisedSocketAddress> advertisedAddress = buildAddresses( coreMembers.keySet() );
        CoreClusterMember coreClusterMember = new CoreClusterMember( memberId, DEFAULT_CLUSTER_SIZE, advertisedAddress,
                discoveryServiceFactory, recordFormat, parentDir,
                extraParams, instanceExtraParams );
        coreMembers.put( memberId, coreClusterMember );
        return coreClusterMember;
    }

    /**
     * Perform a transaction against the leader of the core cluster, retrying as necessary.
     */
    private CoreClusterMember leaderTx( BiConsumer<CoreGraphDatabase,Transaction> op, int timeout, TimeUnit timeUnit )
            throws Exception
    {
        ThrowingSupplier<CoreClusterMember,Exception> supplier = () -> {
            CoreClusterMember member = awaitLeader( timeout, timeUnit );
            CoreGraphDatabase db = member.database();
            if ( db == null )
            {
                throw new DatabaseShutdownException();
            }

            try ( Transaction tx = db.beginTx() )
            {
                op.accept( db, tx );
                return member;
            }
            catch ( Throwable e )
            {
                if ( isTransientFailure( e ) )
                {
                    // this is not the best, but it helps in debugging
                    System.err.println( "Transient failure in leader transaction, trying again." );
                    e.printStackTrace();
                    return null;
                }
                else
                {
                    throw e;
                }
            }
        };
        return awaitEx( supplier, notNull()::test, timeout, timeUnit );
    }

    private boolean isTransientFailure( Throwable e )
    {
        // TODO: This should really catch all cases of transient failures. Must be able to express that in a clearer
        // manner...
        return (e instanceof IdGenerationException) || isLockExpired( e ) || isLockOnFollower( e ) ||
                isWriteNotOnLeader( e );

    }

    private boolean isWriteNotOnLeader( Throwable e )
    {
        return e instanceof WriteOperationsNotAllowedException &&
                e.getMessage().startsWith( String.format( LeaderCanWrite.NOT_LEADER_ERROR_MSG, "" ) );
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

    public static List<AdvertisedSocketAddress> buildAddresses( Set<Integer> coreServerIds )
    {
        return coreServerIds.stream().map( Cluster::socketAddressForServer ).collect( toList() );
    }

    public static AdvertisedSocketAddress socketAddressForServer( int id )
    {
        return new AdvertisedSocketAddress( "localhost", (5000 + id) );
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

    private void startReadReplicas( ExecutorService executor ) throws InterruptedException, ExecutionException
    {
        CompletionService<ReadReplicaGraphDatabase> rrcs = new ExecutorCompletionService<>( executor );

        for ( ReadReplica readReplicas : this.readReplicas.values() )
        {
            rrcs.submit( () ->
            {
                readReplicas.start();
                return readReplicas.database();
            } );
        }

        for ( int i = 0; i < readReplicas.size(); i++ )
        {
            rrcs.take().get();
        }
    }

    private void createReadReplicas( int noOfReadReplicas,
                                    final List<AdvertisedSocketAddress> coreMemberAddresses,
                                    Map<String, String> extraParams,
                                    Map<String, IntFunction<String>> instanceExtraParams,
                                    String recordFormat )
    {
        for ( int i = 0; i < noOfReadReplicas; i++ )
        {
            readReplicas.put( i, new ReadReplica( parentDir, i, discoveryServiceFactory, coreMemberAddresses,
                    extraParams, instanceExtraParams, recordFormat ) );
        }
    }

    private void shutdownReadReplicas()
    {
        readReplicas.values().forEach( ReadReplica::shutdown );
    }

    /**
     * Waits for {@link #DEFAULT_TIMEOUT_MS} for the <code>targetDBs</code> to have the same content as the
     * <code>member</code>. Changes in the <code>member</code> database contents after this method is called do not get
     * picked up and are not part of the comparison.
     * @param member The database to check against
     * @param targetDBs The databases expected to match the contents of <code>member</code>
     */
    public static void dataMatchesEventually( CoreClusterMember member, Collection<CoreClusterMember> targetDBs )
            throws TimeoutException, InterruptedException
    {
        dataMatchesEventually( DbRepresentation.of( member.database() ), targetDBs );
    }

    /**
     * Waits for {@link #DEFAULT_TIMEOUT_MS} for the <code>memberThatChanges</code> to match the contents of
     * <code>memberToLookLike</code>. After calling this method, changes both in <code>memberThatChanges</code> and
     * <code>memberToLookLike</code> are picked up.
     */
    public static void dataOnMemberEventuallyLooksLike( CoreClusterMember memberThatChanges,
                                                        CoreClusterMember memberToLookLike )
            throws TimeoutException, InterruptedException
    {
        await( () -> {
                try
                {
                    // We recalculate the DbRepresentation of both source and target, so changes can be picked up
                    DbRepresentation representationToLookLike = DbRepresentation.of( memberToLookLike.database() );
                    DbRepresentation representationThatChanges = DbRepresentation.of( memberThatChanges.database() );
                    return representationToLookLike.equals( representationThatChanges );
                }
                catch( DatabaseShutdownException e )
                {
                    /*
                     * This can happen if the database is still in the process of starting. Yes, the naming
                     * of the exception is unfortunate, since it is thrown when the database lifecycle is not
                     * in RUNNING state and therefore signals general unavailability (e.g still starting) and not
                     * necessarily a database that is shutting down.
                     */
                }
                return false;
            },
            DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    public static void dataMatchesEventually( DbRepresentation sourceRepresentation, Collection<CoreClusterMember> targetDBs )
            throws TimeoutException, InterruptedException
    {
        for ( CoreClusterMember targetDB : targetDBs )
        {
            await( () -> {
                DbRepresentation representation = DbRepresentation.of( targetDB.database() );
                return sourceRepresentation.equals( representation );
            }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
        }
    }

    public void startCoreMembers() throws ExecutionException, InterruptedException
    {
        ExecutorService executor = Executors.newCachedThreadPool( new NamedThreadFactory( "core-starter" ) );
        try
        {
            startCoreMembers( executor );
        }
        finally
        {
            executor.shutdown();
        }
    }

    public ClusterMember getMemberByBoltAddress( AdvertisedSocketAddress advertisedSocketAddress )
    {
        for ( CoreClusterMember member : coreMembers.values() )
        {
            if ( member.boltAdvertisedAddress().equals( advertisedSocketAddress.toString() ) )
            {
                return member;
            }
        }

        for ( ReadReplica member : readReplicas.values() )
        {
            if ( member.boltAdvertisedAddress().equals( advertisedSocketAddress.toString() ) )
            {
                return member;
            }
        }

        throw new RuntimeException( "Could not find a member for bolt address " + advertisedSocketAddress );
    }
}
