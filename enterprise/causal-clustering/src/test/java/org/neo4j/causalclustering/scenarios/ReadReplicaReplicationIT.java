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
package org.neo4j.causalclustering.scenarios;

import org.hamcrest.Matchers;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.log.segmented.FileNames;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.ClusterMember;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.kernel.AvailabilityGuard;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.txtracking.TransactionIdTracker;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.util.UnsatisfiedDependencyException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.time.Duration.ofSeconds;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toSet;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.causalclustering.scenarios.SampleData.createData;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.test.assertion.Assert.assertEventually;

/**
 * Note that this test is extended in the blockdevice repository.
 */
public class ReadReplicaReplicationIT
{
    // This test is extended in the blockdevice repository, and these constants are required there as well.
    protected static final int NR_CORE_MEMBERS = 3;
    protected static final int NR_READ_REPLICAS = 1;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( NR_CORE_MEMBERS )
            .withNumberOfReadReplicas( NR_READ_REPLICAS )
            .withSharedCoreParam( CausalClusteringSettings.cluster_topology_refresh, "5s" )
            .withDiscoveryServiceType( DiscoveryServiceType.HAZELCAST );

    @Test
    public void shouldNotBeAbleToWriteToReadReplica() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        ReadReplicaGraphDatabase readReplica = cluster.findAnyReadReplica().database();

        // when
        try ( Transaction tx = readReplica.beginTx() )
        {
            Node node = readReplica.createNode();
            node.setProperty( "foobar", "baz_bat" );
            node.addLabel( Label.label( "Foo" ) );
            tx.success();
            fail( "should have thrown" );
        }
        catch ( WriteOperationsNotAllowedException e )
        {
            // then all good
        }
    }

    @Test
    public void allServersBecomeAvailable() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // then
        for ( final ReadReplica readReplica : cluster.readReplicas() )
        {
            ThrowingSupplier<Boolean,Exception> availability = () -> readReplica.database().isAvailable( 0 );
            assertEventually( "read replica becomes available", availability, is( true ), 10, SECONDS );
        }
    }

    @Test
    public void shouldEventuallyPullTransactionDownToAllReadReplicas() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();
        int nodesBeforeReadReplicaStarts = 1;

        cluster.coreTx( ( db, tx ) ->
        {
            db.schema().constraintFor( Label.label( "Foo" ) ).assertPropertyIsUnique( "foobar" ).create();
            tx.success();
        } );

        // when
        for ( int i = 0; i < 100; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                createData( db, nodesBeforeReadReplicaStarts );
                tx.success();
            } );
        }

        Set<Path> labelScanStoreFiles = new HashSet<>();
        cluster.coreTx( ( db, tx ) -> gatherLabelScanStoreFiles( db, labelScanStoreFiles ) );

        AtomicBoolean labelScanStoreCorrectlyPlaced = new AtomicBoolean( false );
        Monitors monitors = new Monitors();
        ReadReplica rr = cluster.addReadReplicaWithIdAndMonitors( 0, monitors );
        Path readReplicateStoreDir = rr.storeDir().toPath().toAbsolutePath();

        monitors.addMonitorListener( (FileCopyMonitor) file ->
        {
            Path relativPath = readReplicateStoreDir.relativize( file.toPath().toAbsolutePath() );
            relativPath = relativPath.subpath( 1, relativPath.getNameCount() );
            if ( labelScanStoreFiles.contains( relativPath ) )
            {
                labelScanStoreCorrectlyPlaced.set( true );
            }
        } );

        rr.start();

        for ( int i = 0; i < 100; i++ )
        {
            cluster.coreTx( ( db, tx ) ->
            {
                createData( db, nodesBeforeReadReplicaStarts );
                tx.success();
            } );
        }

        // then
        for ( final ReadReplica server : cluster.readReplicas() )
        {
            GraphDatabaseService readReplica = server.database();
            try ( Transaction tx = readReplica.beginTx() )
            {
                ThrowingSupplier<Long,Exception> nodeCount = () -> count( readReplica.getAllNodes() );
                assertEventually( "node to appear on read replica", nodeCount, is( 400L ) , 1, MINUTES );

                for ( Node node : readReplica.getAllNodes() )
                {
                    assertThat( node.getProperty( "foobar" ).toString(), startsWith( "baz_bat" ) );
                }

                tx.success();
            }
        }

        assertTrue( labelScanStoreCorrectlyPlaced.get() );
    }

    private void gatherLabelScanStoreFiles( GraphDatabaseAPI db, Set<Path> labelScanStoreFiles )
    {
        Path dbStoreDirectory = db.getStoreDir().toPath().toAbsolutePath();
        LabelScanStore labelScanStore = db.getDependencyResolver().resolveDependency( LabelScanStore.class );
        try ( ResourceIterator<File> files = labelScanStore.snapshotStoreFiles() )
        {
            Path relativePath = dbStoreDirectory.relativize( files.next().toPath().toAbsolutePath() );
            labelScanStoreFiles.add( relativePath );
        }
    }

    @Test
    public void shouldShutdownRatherThanPullUpdatesFromCoreMemberWithDifferentStoreIdIfLocalStoreIsNonEmpty()
            throws Exception
    {
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();

        cluster.coreTx( createSomeData );

        cluster.awaitCoreMemberWithRole( Role.FOLLOWER, 2, TimeUnit.SECONDS );

        // Get a read replica and make sure that it is operational
        ReadReplica readReplica = cluster.addReadReplicaWithId( 4 );
        readReplica.start();
        readReplica.database().beginTx().close();

        // Change the store id, so it should fail to join the cluster again
        changeStoreId( readReplica );
        readReplica.shutdown();

        try
        {
            readReplica.start();
            fail( "Should have failed to start" );
        }
        catch ( RuntimeException required )
        {
            // Lifecycle should throw exception, server should not start.
            assertThat( required.getCause(), instanceOf( LifecycleException.class ) );
            assertThat( required.getCause().getCause(), instanceOf( Exception.class ) );
            assertThat( required.getCause().getCause().getMessage(),
                    containsString( "This read replica cannot join the cluster. " +
                            "The local database is not empty and has a mismatching storeId:" ) );
        }
    }

    @Test
    public void aReadReplicShouldBeAbleToRejoinTheCluster() throws Exception
    {
        int readReplicaId = 4;
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();

        cluster.coreTx( createSomeData );

        cluster.addReadReplicaWithId( readReplicaId ).start();

        // let's spend some time by adding more data
        cluster.coreTx( createSomeData );

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );
        cluster.removeReadReplicaWithMemberId( readReplicaId );

        // let's spend some time by adding more data
        cluster.coreTx( createSomeData );

        cluster.addReadReplicaWithId( readReplicaId ).start();

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        Function<ClusterMember,DbRepresentation> toRep = db -> DbRepresentation.of( db.database() );
        Set<DbRepresentation> dbs = cluster.coreMembers().stream().map( toRep ).collect( toSet() );
        dbs.addAll( cluster.readReplicas().stream().map( toRep ).collect( toSet() ) );

        cluster.shutdown();

        assertEquals( 1, dbs.size() );
    }

    @Test
    public void readReplicasShouldRestartIfTheWholeClusterIsRestarted() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // when
        cluster.shutdown();
        cluster.start();

        // then
        for ( final ReadReplica readReplica : cluster.readReplicas() )
        {
            ThrowingSupplier<Boolean,Exception> availability = () -> readReplica.database().isAvailable( 0 );
            assertEventually( "read replica becomes available", availability, is( true ), 10, SECONDS );
        }
    }

    @Test
    public void shouldBeAbleToDownloadANewStoreAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none",
                GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                GraphDatabaseSettings.check_point_interval_time.name(), "100ms" );

        Cluster cluster = clusterRule.withSharedCoreParams( params ).startCluster();

        cluster.coreTx( ( db, tx ) ->
        {
            createData( db, 10 );
            tx.success();
        } );

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        ReadReplica readReplica = cluster.getReadReplicaById( 0 );
        long highestReadReplicaLogVersion = physicalLogFiles( readReplica ).getHighestLogVersion();

        // when
        readReplica.shutdown();

        CoreClusterMember core;
        do
        {
            core = cluster.coreTx( ( db, tx ) ->
            {
                createData( db, 1_000 );
                tx.success();
            } );

        }
        while ( physicalLogFiles( core ).getLowestLogVersion() <= highestReadReplicaLogVersion );

        readReplica.start();

        // then
        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        assertEventually( "The read replica has the same data as the core members",
                () -> DbRepresentation.of( readReplica.database() ),
                equalTo( DbRepresentation.of( cluster.awaitLeader().database() ) ), 10, TimeUnit.SECONDS );
    }

    @Test
    public void shouldBeAbleToPullTxAfterHavingDownloadedANewStoreAfterPruning() throws Exception
    {
        // given
        Map<String,String> params = stringMap( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none",
                GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                GraphDatabaseSettings.check_point_interval_time.name(), "100ms" );

        Cluster cluster = clusterRule.withSharedCoreParams( params ).startCluster();

        cluster.coreTx( ( db, tx ) ->
        {
            createData( db, 10 );
            tx.success();
        } );

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        ReadReplica readReplica = cluster.getReadReplicaById( 0 );
        long highestReadReplicaLogVersion = physicalLogFiles( readReplica ).getHighestLogVersion();

        readReplica.shutdown();

        CoreClusterMember core;
        do
        {
            core = cluster.coreTx( ( db, tx ) ->
            {
                createData( db, 1_000 );
                tx.success();
            } );

        }
        while ( physicalLogFiles( core ).getLowestLogVersion() <= highestReadReplicaLogVersion );

        readReplica.start();

        awaitEx( () -> readReplicasUpToDateAsTheLeader( cluster.awaitLeader(), cluster.readReplicas() ), 1, TimeUnit.MINUTES );

        // when
        cluster.coreTx( ( db, tx ) ->
        {
            createData( db, 10 );
            tx.success();
        } );

        // then
        assertEventually( "The read replica has the same data as the core members",
                () -> DbRepresentation.of( readReplica.database() ),
                equalTo( DbRepresentation.of( cluster.awaitLeader().database() ) ), 10, TimeUnit.SECONDS );
    }

    @Test
    public void transactionsShouldNotAppearOnTheReadReplicaWhilePollingIsPaused() throws Throwable
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        ReadReplicaGraphDatabase readReplicaGraphDatabase = cluster.findAnyReadReplica().database();
        CatchupPollingProcess pollingClient = readReplicaGraphDatabase.getDependencyResolver()
                .resolveDependency( CatchupPollingProcess.class );
        pollingClient.stop();

        cluster.coreTx( ( coreGraphDatabase, transaction ) ->
        {
            coreGraphDatabase.createNode();
            transaction.success();
        } );

        CoreGraphDatabase leaderDatabase = cluster.awaitLeader().database();
        long transactionVisibleOnLeader = transactionIdTracker( leaderDatabase ).newestEncounteredTxId();

        // when the poller is paused, transaction doesn't make it to the read replica
        try
        {
            transactionIdTracker( readReplicaGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 15 ) );
            fail( "should have thrown exception" );
        }
        catch ( TransactionFailureException e )
        {
            // expected timeout
        }

        // when the poller is resumed, it does make it to the read replica
        pollingClient.start();
        transactionIdTracker( readReplicaGraphDatabase ).awaitUpToDate( transactionVisibleOnLeader, ofSeconds( 15 ) );
    }

    private TransactionIdTracker transactionIdTracker( GraphDatabaseAPI database )
    {
        Supplier<TransactionIdStore> transactionIdStore =
                database.getDependencyResolver().provideDependency( TransactionIdStore.class );
        AvailabilityGuard availabilityGuard =
                database.getDependencyResolver().resolveDependency( AvailabilityGuard.class );
        return new TransactionIdTracker( transactionIdStore, availabilityGuard );
    }

    private LogFiles physicalLogFiles( ClusterMember clusterMember )
    {
        return clusterMember.database().getDependencyResolver().resolveDependency( LogFiles.class );
    }

    private boolean readReplicasUpToDateAsTheLeader( CoreClusterMember leader,
            Collection<ReadReplica> readReplicas )
    {
        long leaderTxId = lastClosedTransactionId( true, leader.database() );
        return readReplicas.stream().map( ReadReplica::database )
                .map( db -> lastClosedTransactionId( false, db ) )
                .reduce( true, ( acc, txId ) -> acc && txId == leaderTxId, Boolean::logicalAnd );
    }

    private void changeStoreId( ReadReplica replica ) throws IOException
    {
        File neoStoreFile = new File( replica.storeDir(), MetaDataStore.DEFAULT_NAME );
        PageCache pageCache = replica.database().getDependencyResolver().resolveDependency( PageCache.class );
        MetaDataStore.setRecord( pageCache, neoStoreFile, TIME, System.currentTimeMillis() );
    }

    private long lastClosedTransactionId( boolean fail, GraphDatabaseFacade db )
    {
        try
        {
            return db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                    .getLastClosedTransactionId();
        }
        catch ( IllegalStateException  | UnsatisfiedDependencyException /* db is shutdown or not available */ ex )
        {
            if ( !fail )
            {
                // the db is down we'll try again...
                return -1;
            }
            else
            {
                throw ex;
            }
        }
    }

    @Test
    public void shouldThrowExceptionIfReadReplicaRecordFormatDiffersToCoreRecordFormat() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).withRecordFormat( HighLimit.NAME ).startCluster();

        // when
        cluster.coreTx( createSomeData );

        try
        {
            String format = Standard.LATEST_NAME;
            cluster.addReadReplicaWithIdAndRecordFormat( 0, format ).start();
            fail( "starting read replica with '" + format + "' format should have failed" );
        }
        catch ( Exception e )
        {
            assertThat( e.getCause().getCause().getMessage(),
                    containsString( "Failed to start database with copied store" ) );
        }
    }

    @Test
    public void shouldBeAbleToCopyStoresFromCoreToReadReplica() throws Exception
    {
        // given
        Map<String,String> params = stringMap( CausalClusteringSettings.raft_log_rotation_size.name(), "1k",
                CausalClusteringSettings.raft_log_pruning_frequency.name(), "500ms",
                CausalClusteringSettings.state_machine_flush_window_size.name(), "1",
                CausalClusteringSettings.raft_log_pruning_strategy.name(), "1 entries" );
        Cluster cluster = clusterRule.withNumberOfReadReplicas( 0 ).withSharedCoreParams( params )
                .withRecordFormat( HighLimit.NAME ).startCluster();

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( Label.label( "L" ) );
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
            }
            tx.success();
        } );

        long baseVersion = versionBy( cluster.awaitLeader().raftLogDirectory(), Math::max );

        CoreClusterMember coreGraphDatabase = null;
        for ( int j = 0; j < 2; j++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode( Label.label( "L" ) );
                for ( int i = 0; i < 10; i++ )
                {
                    node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
                }
                tx.success();
            } );
        }

        File raftLogDir = coreGraphDatabase.raftLogDirectory();
        assertEventually( "pruning happened", () -> versionBy( raftLogDir, Math::min ), greaterThan( baseVersion ), 5,
                SECONDS );

        // when
        cluster.addReadReplicaWithIdAndRecordFormat( 4, HighLimit.NAME ).start();

        // then
        for ( final ReadReplica readReplica : cluster.readReplicas() )
        {
            assertEventually( "read replica available", () -> readReplica.database().isAvailable( 0 ), is( true ), 10,
                    SECONDS );
        }
    }

    private long versionBy( File raftLogDir, BinaryOperator<Long> operator ) throws IOException
    {
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction() )
        {
            SortedMap<Long,File> logs = new FileNames( raftLogDir ).getAllFiles( fileSystem, mock( Log.class ) );
            return logs.keySet().stream().reduce( operator ).orElseThrow( IllegalStateException::new );
        }
    }

    @Test
    public void pageFaultsFromReplicationMustCountInMetrics() throws Exception
    {
        // Given initial pin counts on all members
        Cluster cluster = clusterRule.startCluster();
        Function<ReadReplica,PageCacheCounters> getPageCacheCounters =
                ccm -> ccm.database().getDependencyResolver().resolveDependency( PageCacheCounters.class );
        List<PageCacheCounters> countersList =
                cluster.readReplicas().stream().map( getPageCacheCounters ).collect( Collectors.toList() );
        long[] initialPins = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();

        // when the leader commits a write transaction,
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then the replication should cause pins on a majority of core members to increase.
        // However, the commit returns as soon as the transaction has been replicated through the Raft log, which
        // happens before the transaction is applied on the members, and then replicated to read-replicas.
        // Therefor we are racing with the transaction application on the read-replicas, so we have to spin.
        int minimumUpdatedMembersCount = countersList.size() / 2 + 1;
        assertEventually( "Expected followers to eventually increase pin counts", () ->
        {
            long[] pinsAfterCommit = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();
            int membersWithIncreasedPinCount = 0;
            for ( int i = 0; i < initialPins.length; i++ )
            {
                long before = initialPins[i];
                long after = pinsAfterCommit[i];
                if ( before < after )
                {
                    membersWithIncreasedPinCount++;
                }
            }
            return membersWithIncreasedPinCount;
        }, Matchers.is( greaterThanOrEqualTo( minimumUpdatedMembersCount ) ), 10, SECONDS );
    }

    private final BiConsumer<CoreGraphDatabase,Transaction> createSomeData = ( db, tx ) ->
    {
        createData( db, 10 );
        tx.success();
    };
}
