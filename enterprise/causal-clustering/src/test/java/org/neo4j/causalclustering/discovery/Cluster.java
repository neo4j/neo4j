/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.discovery;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.LeaderCanWrite;
import org.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.core.state.machines.id.IdGenerationException;
import org.neo4j.causalclustering.core.state.machines.locks.LeaderOnlyLockManager;
import org.neo4j.causalclustering.helper.ErrorHandler;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.test.DbRepresentation;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.neo4j.concurrent.Futures.combine;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.function.Predicates.notNull;
import static org.neo4j.helpers.collection.Iterables.firstOrNull;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.LockSessionExpired;

public class Cluster
{
    private static final int DEFAULT_TIMEOUT_MS = 120_000;
    private static final int DEFAULT_CLUSTER_SIZE = 3;

    protected final File parentDir;
    private final Map<String,String> coreParams;
    private final Map<String,IntFunction<String>> instanceCoreParams;
    private final Map<String,String> readReplicaParams;
    private final Map<String,IntFunction<String>> instanceReadReplicaParams;
    private final String recordFormat;
    protected final DiscoveryServiceFactory discoveryServiceFactory;
    protected final String listenAddress;
    protected final String advertisedAddress;
    private final Set<String> dbNames;

    private Map<Integer,CoreClusterMember> coreMembers = new ConcurrentHashMap<>();
    private Map<Integer,ReadReplica> readReplicas = new ConcurrentHashMap<>();
    private int highestCoreServerId;
    private int highestReplicaServerId;

    public Cluster( File parentDir, int noOfCoreMembers, int noOfReadReplicas,
            DiscoveryServiceFactory discoveryServiceFactory,
            Map<String,String> coreParams, Map<String,IntFunction<String>> instanceCoreParams,
            Map<String,String> readReplicaParams, Map<String,IntFunction<String>> instanceReadReplicaParams,
            String recordFormat, IpFamily ipFamily, boolean useWildcard )
    {
        this( parentDir, noOfCoreMembers, noOfReadReplicas, discoveryServiceFactory, coreParams,
                instanceCoreParams, readReplicaParams, instanceReadReplicaParams, recordFormat, ipFamily,
                useWildcard, Collections.singleton( CausalClusteringSettings.database.getDefaultValue() ) );
    }

    public Cluster( File parentDir, int noOfCoreMembers, int noOfReadReplicas,
            DiscoveryServiceFactory discoveryServiceFactory,
            Map<String,String> coreParams, Map<String,IntFunction<String>> instanceCoreParams,
            Map<String,String> readReplicaParams, Map<String,IntFunction<String>> instanceReadReplicaParams,
            String recordFormat, IpFamily ipFamily, boolean useWildcard, Set<String> dbNames )
    {
        this.discoveryServiceFactory = discoveryServiceFactory;
        this.parentDir = parentDir;
        this.coreParams = coreParams;
        this.instanceCoreParams = instanceCoreParams;
        this.readReplicaParams = readReplicaParams;
        this.instanceReadReplicaParams = instanceReadReplicaParams;
        this.recordFormat = recordFormat;
        listenAddress = useWildcard ? ipFamily.wildcardAddress() : ipFamily.localhostAddress();
        advertisedAddress = ipFamily.localhostName();
        List<AdvertisedSocketAddress> initialHosts = initialHosts( noOfCoreMembers );
        createCoreMembers( noOfCoreMembers, initialHosts, coreParams, instanceCoreParams, recordFormat );
        createReadReplicas( noOfReadReplicas, initialHosts, readReplicaParams, instanceReadReplicaParams, recordFormat );
        this.dbNames = dbNames;
    }

    private List<AdvertisedSocketAddress> initialHosts( int noOfCoreMembers )
    {
        return IntStream.range( 0, noOfCoreMembers )
                .mapToObj( ignored -> PortAuthority.allocatePort() )
                .map( port -> new AdvertisedSocketAddress( advertisedAddress, port ) )
                .collect( toList() );
    }

    public void start() throws InterruptedException, ExecutionException
    {
        startCoreMembers();
        startReadReplicas();
    }

    public Set<CoreClusterMember> healthyCoreMembers()
    {
        return coreMembers.values().stream()
                .filter( db -> db.database().getDependencyResolver().resolveDependency( DatabaseHealth.class ).isHealthy() )
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
        return addCoreMemberWithId( memberId, coreParams, instanceCoreParams, recordFormat );
    }

    public CoreClusterMember newCoreMember()
    {
        int newCoreServerId = ++highestCoreServerId;
        return addCoreMemberWithId( newCoreServerId );
    }

    public ReadReplica newReadReplica()
    {
        int newReplicaServerId = ++highestReplicaServerId;
        return addReadReplicaWithId( newReplicaServerId );
    }

    private CoreClusterMember addCoreMemberWithId( int memberId, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        List<AdvertisedSocketAddress> initialHosts = extractInitialHosts( coreMembers );
        CoreClusterMember coreClusterMember = createCoreClusterMember(
                memberId,
                PortAuthority.allocatePort(),
                DEFAULT_CLUSTER_SIZE,
                initialHosts,
                recordFormat,
                extraParams,
                instanceExtraParams
        );

        coreMembers.put( memberId, coreClusterMember );
        return coreClusterMember;
    }

    public ReadReplica addReadReplicaWithIdAndRecordFormat( int memberId, String recordFormat )
    {
        return addReadReplica( memberId, recordFormat, new Monitors() );
    }

    public ReadReplica addReadReplicaWithId( int memberId )
    {
        return addReadReplicaWithIdAndRecordFormat( memberId, recordFormat );
    }

    public ReadReplica addReadReplicaWithIdAndMonitors( @SuppressWarnings( "SameParameterValue" ) int memberId, Monitors monitors )
    {
        return addReadReplica( memberId, recordFormat, monitors );
    }

    private ReadReplica addReadReplica( int memberId, String recordFormat, Monitors monitors )
    {
        List<AdvertisedSocketAddress> initialHosts = extractInitialHosts( coreMembers );
        ReadReplica member = createReadReplica(
                memberId,
                initialHosts,
                readReplicaParams,
                instanceReadReplicaParams,
                recordFormat,
                monitors
        );

        readReplicas.put( memberId, member );
        return member;
    }

    public void shutdown()
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( "Error when trying to shutdown cluster" ) )
        {
            shutdownCoreMembers( coreMembers(), errorHandler );
            shutdownReadReplicas( errorHandler );
        }
    }

    private void shutdownCoreMembers( Collection<CoreClusterMember> members, ErrorHandler errorHandler )
    {
        shutdownMembers( members, errorHandler );
    }

    public void shutdownCoreMembers()
    {
        shutdownCoreMembers( coreMembers() );
    }

    public void shutdownCoreMember( CoreClusterMember member )
    {
        shutdownCoreMembers( Collections.singleton( member ) );
    }

    public void shutdownCoreMembers( Collection<CoreClusterMember> members )
    {
        try ( ErrorHandler errorHandler = new ErrorHandler( "Error when trying to shutdown core members" ) )
        {
            shutdownCoreMembers( members, errorHandler );
        }
    }

    @SuppressWarnings( "unchecked" )
    private void shutdownMembers( Collection<? extends ClusterMember> clusterMembers, ErrorHandler errorHandler )
    {
        try
        {
            combine( invokeAll( "cluster-shutdown", clusterMembers, cm ->
            {
                cm.shutdown();
                return null;
            } ) ).get();
        }
        catch ( Exception e )
        {
            errorHandler.add( e );
        }
    }

    private <X extends GraphDatabaseAPI, T extends ClusterMember<X>, R> List<Future<R>> invokeAll(
            String threadName, Collection<T> members, Function<T,R> call )
    {
        List<Future<R>> list = new ArrayList<>( members.size() );
        int threadNumber = 0;
        for ( T member : members )
        {
            FutureTask<R> task = new FutureTask<>( () -> call.apply( member ) );
            ThreadGroup threadGroup = member.threadGroup();
            Thread thread = new Thread( threadGroup, task, threadName + "-" + threadNumber );
            thread.start();
            threadNumber++;
            list.add( task );
        }
        return list;
    }

    public void removeCoreMemberWithServerId( int serverId )
    {
        CoreClusterMember memberToRemove = getCoreMemberById( serverId );

        if ( memberToRemove != null )
        {
            memberToRemove.shutdown();
            removeCoreMember( memberToRemove );
        }
        else
        {
            throw new RuntimeException( "Could not remove core member with id " + serverId );
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

    private void ensureDBName( String dbName ) throws IllegalArgumentException
    {
        if ( !dbNames.contains( dbName ) )
        {
            throw new IllegalArgumentException( "Database name " + dbName + " does not exist in this cluster." );
        }
    }

    public CoreClusterMember getMemberWithRole( Role role )
    {
        return getMemberWithAnyRole( role );
    }

    public List<CoreClusterMember> getAllMembersWithRole( Role role )
    {
        return getAllMembersWithAnyRole( role );
    }

    public CoreClusterMember getMemberWithRole( String dbName, Role role )
    {
        return getMemberWithAnyRole( dbName, role );
    }

    public List<CoreClusterMember> getAllMembersWithRole( String dbName, Role role )
    {
        return getAllMembersWithAnyRole( dbName, role );
    }

    public CoreClusterMember getMemberWithAnyRole( Role... roles )
    {
        String dbName = CausalClusteringSettings.database.getDefaultValue();
        return getMemberWithAnyRole( dbName, roles );
    }

    public List<CoreClusterMember> getAllMembersWithAnyRole( Role... roles )
    {
        String dbName = CausalClusteringSettings.database.getDefaultValue();
        return getAllMembersWithAnyRole( dbName, roles );
    }

    public CoreClusterMember getMemberWithAnyRole( String dbName, Role... roles )
    {
        return getAllMembersWithAnyRole( dbName, roles ).stream().findFirst().orElse( null );
    }

    public List<CoreClusterMember> getAllMembersWithAnyRole( String dbName, Role... roles )
    {
        ensureDBName( dbName );
        Set<Role> roleSet = Arrays.stream( roles ).collect( toSet() );

        List<CoreClusterMember> list = new ArrayList<>();
        for ( CoreClusterMember m : coreMembers.values() )
        {
            CoreGraphDatabase database = m.database();
            if ( database == null )
            {
                continue;
            }

            if ( m.dbName().equals( dbName ) )
            {
                if ( roleSet.contains( database.getRole() ) )
                {
                    list.add( m );
                }
            }
        }
        return list;
    }

    public CoreClusterMember awaitLeader() throws TimeoutException
    {
        return awaitCoreMemberWithRole( Role.LEADER, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    public CoreClusterMember awaitLeader( String dbName ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( dbName, Role.LEADER, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    public CoreClusterMember awaitLeader( String dbName, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( dbName, Role.LEADER, timeout, timeUnit );
    }

    public CoreClusterMember awaitLeader( long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return awaitCoreMemberWithRole( Role.LEADER, timeout, timeUnit );
    }

    public CoreClusterMember awaitCoreMemberWithRole( Role role, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return await( () -> getMemberWithRole( role ), notNull(), timeout, timeUnit );
    }

    public CoreClusterMember awaitCoreMemberWithRole( String dbName, Role role, long timeout, TimeUnit timeUnit ) throws TimeoutException
    {
        return await( () -> getMemberWithRole( dbName, role ), notNull(), timeout, timeUnit );
    }

    public int numberOfCoreMembersReportedByTopology()
    {

        CoreClusterMember aCoreGraphDb = coreMembers.values().stream()
                .filter( member -> member.database() != null ).findAny().orElseThrow( IllegalArgumentException::new );
        CoreTopologyService coreTopologyService = aCoreGraphDb.database().getDependencyResolver()
                .resolveDependency( CoreTopologyService.class );
        return coreTopologyService.localCoreServers().members().size();
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreClusterMember coreTx( BiConsumer<CoreGraphDatabase,Transaction> op ) throws Exception
    {
        String dbName = CausalClusteringSettings.database.getDefaultValue();
        return coreTx( dbName, op );
    }

    /**
     * Perform a transaction against the core cluster, selecting the target and retrying as necessary.
     */
    public CoreClusterMember coreTx( String dbName, BiConsumer<CoreGraphDatabase,Transaction> op ) throws Exception
    {
        ensureDBName( dbName );
        return leaderTx( dbName, op, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
    }

    /**
     * Perform a transaction against the leader of the core cluster, retrying as necessary.
     */
    private CoreClusterMember leaderTx( String dbName, BiConsumer<CoreGraphDatabase,Transaction> op, int timeout, TimeUnit timeUnit )
            throws Exception
    {
        ThrowingSupplier<CoreClusterMember,Exception> supplier = () ->
        {
            CoreClusterMember member = awaitLeader( dbName, timeout, timeUnit );
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
        Predicate<Throwable> throwablePredicate =
                e1 -> isLockExpired( e1 ) || isLockOnFollower( e1 ) || isWriteNotOnLeader( e1 ) || e1 instanceof TransientTransactionFailureException ||
                        e1 instanceof IdGenerationException;
        return Exceptions.contains( e, throwablePredicate );
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
               e.getCause() instanceof org.neo4j.internal.kernel.api.exceptions.TransactionFailureException &&
               ((org.neo4j.internal.kernel.api.exceptions.TransactionFailureException) e.getCause()).status() ==
               LockSessionExpired;
    }

    private List<AdvertisedSocketAddress> extractInitialHosts( Map<Integer,CoreClusterMember> coreMembers )
    {
        return coreMembers.values().stream()
                .map( CoreClusterMember::discoveryPort )
                .map( port -> new AdvertisedSocketAddress( advertisedAddress, port ) )
                .collect( toList() );
    }

    private void createCoreMembers( final int noOfCoreMembers,
            List<AdvertisedSocketAddress> initialHosts, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat )
    {
        for ( int i = 0; i < initialHosts.size(); i++ )
        {
            int discoveryListenAddress = initialHosts.get( i ).getPort();
            CoreClusterMember coreClusterMember = createCoreClusterMember(
                    i,
                    discoveryListenAddress,
                    noOfCoreMembers,
                    initialHosts,
                    recordFormat,
                    extraParams,
                    instanceExtraParams
            );
            coreMembers.put( i, coreClusterMember );
        }
        highestCoreServerId = noOfCoreMembers - 1;
    }

    protected CoreClusterMember createCoreClusterMember( int serverId,
                                                       int hazelcastPort,
                                                       int clusterSize,
                                                       List<AdvertisedSocketAddress> initialHosts,
                                                       String recordFormat,
                                                       Map<String, String> extraParams,
                                                       Map<String, IntFunction<String>> instanceExtraParams )
    {
        int txPort = PortAuthority.allocatePort();
        int raftPort = PortAuthority.allocatePort();
        int boltPort = PortAuthority.allocatePort();
        int httpPort = PortAuthority.allocatePort();
        int backupPort = PortAuthority.allocatePort();

        return new CoreClusterMember(
                serverId,
                hazelcastPort,
                txPort,
                raftPort,
                boltPort,
                httpPort,
                backupPort,
                clusterSize,
                initialHosts,
                discoveryServiceFactory,
                recordFormat,
                parentDir,
                extraParams,
                instanceExtraParams,
                listenAddress,
                advertisedAddress
        );
    }

    protected ReadReplica createReadReplica( int serverId,
                                           List<AdvertisedSocketAddress> initialHosts,
                                           Map<String, String> extraParams,
                                           Map<String, IntFunction<String>> instanceExtraParams,
                                           String recordFormat,
                                           Monitors monitors )
    {
        int boltPort = PortAuthority.allocatePort();
        int httpPort = PortAuthority.allocatePort();
        int txPort = PortAuthority.allocatePort();
        int backupPort = PortAuthority.allocatePort();

        return new ReadReplica(
                parentDir,
                serverId,
                boltPort,
                httpPort,
                txPort,
                backupPort, discoveryServiceFactory,
                initialHosts,
                extraParams,
                instanceExtraParams,
                recordFormat,
                monitors,
                advertisedAddress,
                listenAddress
        );
    }

    public void startCoreMembers() throws InterruptedException, ExecutionException
    {
        startCoreMembers( coreMembers.values() );
    }

    public void startCoreMember( CoreClusterMember member ) throws InterruptedException, ExecutionException
    {
        startCoreMembers( Collections.singleton( member ) );
    }

    public void startCoreMembers( Collection<CoreClusterMember> members ) throws InterruptedException, ExecutionException
    {
        List<Future<CoreGraphDatabase>> futures = invokeAll( "cluster-starter", members, cm ->
        {
            cm.start();
            return cm.database();
        } );
        for ( Future<CoreGraphDatabase> future : futures )
        {
            future.get();
        }
    }

    private void startReadReplicas() throws InterruptedException, ExecutionException
    {
        Collection<ReadReplica> members = readReplicas.values();
        List<Future<ReadReplicaGraphDatabase>> futures = invokeAll( "cluster-starter", members, cm ->
        {
            cm.start();
            return cm.database();
        } );
        for ( Future<ReadReplicaGraphDatabase> future : futures )
        {
            future.get();
        }
    }

    private void createReadReplicas( int noOfReadReplicas,
            final List<AdvertisedSocketAddress> initialHosts,
            Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams,
            String recordFormat )
    {
        for ( int i = 0; i < noOfReadReplicas; i++ )
        {
            ReadReplica readReplica = createReadReplica(
                    i,
                    initialHosts,
                    extraParams,
                    instanceExtraParams,
                    recordFormat,
                    new Monitors()
            );

            readReplicas.put( i, readReplica );
        }
        highestReplicaServerId = noOfReadReplicas - 1;
    }

    private void shutdownReadReplicas( ErrorHandler errorHandler )
    {
        shutdownMembers( readReplicas(), errorHandler );
    }

    /**
     * Waits for {@link #DEFAULT_TIMEOUT_MS} for the <code>memberThatChanges</code> to match the contents of
     * <code>memberToLookLike</code>. After calling this method, changes both in <code>memberThatChanges</code> and
     * <code>memberToLookLike</code> are picked up.
     */
    public static void dataOnMemberEventuallyLooksLike( CoreClusterMember memberThatChanges,
            CoreClusterMember memberToLookLike )
            throws TimeoutException
    {
        await( () ->
                {
                    try
                    {
                        // We recalculate the DbRepresentation of both source and target, so changes can be picked up
                        DbRepresentation representationToLookLike = DbRepresentation.of( memberToLookLike.database() );
                        DbRepresentation representationThatChanges = DbRepresentation.of( memberThatChanges.database() );
                        return representationToLookLike.equals( representationThatChanges );
                    }
                    catch ( DatabaseShutdownException e )
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

    public static <T extends ClusterMember> void dataMatchesEventually( ClusterMember source, Collection<T> targets )
            throws TimeoutException
    {
        dataMatchesEventually( DbRepresentation.of( source.database() ), targets );
    }

    /**
     * Waits for {@link #DEFAULT_TIMEOUT_MS} for the <code>targetDBs</code> to have the same content as the
     * <code>member</code>. Changes in the <code>member</code> database contents after this method is called do not get
     * picked up and are not part of the comparison.
     *
     * @param source  The database to check against
     * @param targets The databases expected to match the contents of <code>member</code>
     */
    public static <T extends ClusterMember> void dataMatchesEventually( DbRepresentation source, Collection<T> targets )
            throws TimeoutException
    {
        for ( ClusterMember targetDB : targets )
        {
            await( () ->
            {
                DbRepresentation representation = DbRepresentation.of( targetDB.database() );
                return source.equals( representation );
            }, DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
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

    public Optional<ClusterMember> randomMember( boolean mustBeStarted )
    {
        Stream<ClusterMember> members = Stream.concat( coreMembers().stream(), readReplicas().stream() );

        if ( mustBeStarted )
        {
            members = members.filter( m -> !m.isShutdown() );
        }

        List<ClusterMember> eligible = members.collect( Collectors.toList() );
        return random( eligible );
    }

    public Optional<CoreClusterMember> randomCoreMember( boolean mustBeStarted )
    {
        Stream<CoreClusterMember> members = coreMembers().stream();

        if ( mustBeStarted )
        {
            members = members.filter( m -> !m.isShutdown() );
        }

        List<CoreClusterMember> eligible = members.collect( Collectors.toList() );
        return random( eligible );
    }

    private static <T> Optional<T> random( List<T> list )
    {
        if ( list.size() == 0 )
        {
            return Optional.empty();
        }
        int ordinal = ThreadLocalRandom.current().nextInt( list.size() );
        return Optional.of( list.get( ordinal ) );
    }
}
