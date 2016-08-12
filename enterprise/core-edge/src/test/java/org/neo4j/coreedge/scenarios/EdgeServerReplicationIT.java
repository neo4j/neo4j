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
package org.neo4j.coreedge.scenarios;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.discovery.EdgeClusterMember;
import org.neo4j.coreedge.core.consensus.log.segmented.FileNames;
import org.neo4j.coreedge.core.consensus.roles.Role;
import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.CoreGraphDatabase;
import org.neo4j.coreedge.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.coreedge.edge.EdgeGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.lock.AcquireLockTimeoutException;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.coreedge.ClusterRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import static org.neo4j.coreedge.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;
import static org.neo4j.coreedge.core.consensus.log.RaftLog.PHYSICAL_LOG_DIRECTORY_NAME;
import static org.neo4j.function.Predicates.awaitEx;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class EdgeServerReplicationIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule( getClass() )
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfEdgeMembers( 1 )
                    .withDiscoveryServiceFactory( new HazelcastDiscoveryServiceFactory() );

    @Test
    public void shouldNotBeAbleToWriteToEdge() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        EdgeGraphDatabase edgeDB = cluster.findAnEdgeMember().database();

        // when (write should fail)
        boolean transactionFailed = false;
        try ( Transaction tx = edgeDB.beginTx() )
        {
            Node node = edgeDB.createNode();
            node.setProperty( "foobar", "baz_bat" );
            node.addLabel( Label.label( "Foo" ) );
            tx.success();
        }
        catch ( TransactionFailureException e )
        {
            // expected
            transactionFailed = true;
        }

        assertTrue( transactionFailed );
    }

    @Test
    public void allServersBecomeAvailable() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        // then
        for ( final EdgeClusterMember edgeClusterMember : cluster.edgeMembers() )
        {
            ThrowingSupplier<Boolean,Exception> availability = () -> edgeClusterMember.database().isAvailable( 0 );
            assertEventually( "edge server becomes available", availability, is( true ), 10, SECONDS );
        }

        Thread.sleep( 20_000 );
    }

    @Test
    public void shouldEventuallyPullTransactionDownToAllEdgeServers() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();
        int nodesBeforeEdgeServerStarts = 1;

        // when
        executeOnLeaderWithRetry( db -> {
            for ( int i = 0; i < nodesBeforeEdgeServerStarts; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
        }, cluster );

        cluster.addEdgeMemberWithId( 0 ).start();

        // when
        executeOnLeaderWithRetry( db -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
        }, cluster );

        // then
        for ( final EdgeClusterMember server : cluster.edgeMembers() )
        {
            GraphDatabaseService edgeDB = server.database();
            try ( Transaction tx = edgeDB.beginTx() )
            {
                ThrowingSupplier<Long,Exception> nodeCount = () -> count( edgeDB.getAllNodes() );
                assertEventually( "node to appear on edge server", nodeCount, is( nodesBeforeEdgeServerStarts + 1L ), 1,
                        MINUTES );

                for ( Node node : edgeDB.getAllNodes() )
                {
                    assertEquals( "baz_bat", node.getProperty( "foobar" ) );
                }

                tx.success();
            }
        }
    }

    @Test
    public void shouldShutdownRatherThanPullUpdatesFromCoreMemberWithDifferentStoreIdIfLocalStoreIsNonEmpty() throws Exception
    {
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();

        executeOnLeaderWithRetry( this::createData, cluster );

        CoreClusterMember follower = cluster.awaitCoreMemberWithRole( 2000, Role.FOLLOWER );
        // Shutdown server before copying its data, because Windows can't copy open files.
        follower.shutdown();

        EdgeClusterMember edgeClusterMember = cluster.addEdgeMemberWithId( 4 );
        putSomeDataWithDifferentStoreId( edgeClusterMember.storeDir(), follower.storeDir() );

        try
        {
            edgeClusterMember.start();
            fail( "Should have failed to start" );
        }
        catch ( RuntimeException required )
        {
            // Lifecycle should throw exception, server should not start.
            assertThat( required.getCause(), instanceOf( LifecycleException.class ) );
            assertThat( required.getCause().getCause(), instanceOf( IllegalStateException.class ) );
            assertThat( required.getCause().getCause().getMessage(),
                    containsString( "This edge machine cannot join the cluster. " +
                            "The local database is not empty and has a mismatching storeId:" ) );
        }
    }

    private boolean edgesUpToDateAsTheLeader( CoreClusterMember leader, Collection<EdgeClusterMember> edgeClusterMembers )
    {
        long leaderTxId = lastClosedTransactionId( leader.database() );
        return edgeClusterMembers.stream().map( EdgeClusterMember::database ).map( this::lastClosedTransactionId )
                .reduce( true, ( acc, txId ) -> acc && txId == leaderTxId, Boolean::logicalAnd );
    }

    private void putSomeDataWithDifferentStoreId( File storeDir, File coreStoreDir ) throws IOException
    {
        FileUtils.copyRecursively( coreStoreDir, storeDir );
        changeStoreId( storeDir );
    }

    private void changeStoreId( File storeDir ) throws IOException
    {
        File neoStoreFile = new File( storeDir, MetaDataStore.DEFAULT_NAME );
        try ( PageCache pageCache = StandalonePageCacheFactory.createPageCache( new DefaultFileSystemAbstraction() ) )
        {
            MetaDataStore.setRecord( pageCache, neoStoreFile, TIME, System.currentTimeMillis() );
        }
    }

    @Test
    public void anEdgeServerShouldBeAbleToRejoinTheCluster() throws Exception
    {
        int edgeServerId = 4;
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();

        executeOnLeaderWithRetry( this::createData, cluster );

        cluster.addEdgeMemberWithId( edgeServerId );

        // let's spend some time by adding more data
        executeOnLeaderWithRetry( this::createData, cluster );

        cluster.removeEdgeMemberWithMemberId( edgeServerId );

        // let's spend some time by adding more data
        executeOnLeaderWithRetry( this::createData, cluster );

        cluster.addEdgeMemberWithId( edgeServerId ).start();

        awaitEx( () -> edgesUpToDateAsTheLeader( cluster.awaitLeader(), cluster.edgeMembers() ), 1, TimeUnit.MINUTES );

        List<File> coreStoreDirs = cluster.coreMembers().stream().map( CoreClusterMember::storeDir ).collect( toList() );
        List<File> edgeStoreDirs = cluster.edgeMembers().stream().map( EdgeClusterMember::storeDir ).collect( toList() );

        cluster.shutdown();

        Set<DbRepresentation> dbs = coreStoreDirs.stream().map( DbRepresentation::of ).collect( toSet() );
        dbs.addAll( edgeStoreDirs.stream().map( DbRepresentation::of ).collect( toSet() ) );
        assertEquals( 1, dbs.size() );
    }

    private long lastClosedTransactionId( GraphDatabaseFacade db )
    {
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();
    }

    @Test
    public void shouldThrowExceptionIfEdgeRecordFormatDiffersToCoreRecordFormat() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 0 ).withRecordFormat( HighLimit.NAME ).startCluster();

        // when
        executeOnLeaderWithRetry( this::createData, cluster );

        try
        {
            cluster.addEdgeMemberWithIdAndRecordFormat( 0, StandardV3_0.NAME );
        }
        catch ( Exception e )
        {
            assertThat( e.getCause().getCause().getMessage(),
                    containsString( "Failed to start database with copied store" ) );
        }
    }

    @Test
    public void shouldBeAbleToCopyStoresFromCoreToEdge() throws Exception
    {
        // given
        Map<String,String> params = stringMap( CoreEdgeClusterSettings.raft_log_rotation_size.name(), "1k",
                CoreEdgeClusterSettings.raft_log_pruning_frequency.name(), "500ms",
                CoreEdgeClusterSettings.state_machine_flush_window_size.name(), "1",
                CoreEdgeClusterSettings.raft_log_pruning_strategy.name(), "1 entries" );
        Cluster cluster = clusterRule.withNumberOfEdgeMembers( 0 ).withSharedCoreParams( params )
                .withRecordFormat( HighLimit.NAME ).startCluster();

        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( Label.label( "L" ) );
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
            }
            tx.success();
        } );

        long baseVersion = versionBy( cluster.awaitLeader().storeDir(), Math::max );

        CoreClusterMember coreGraphDatabase = null;
        for ( int j = 0; j < 2; j++ )
        {
            coreGraphDatabase = cluster.coreTx( ( db, tx ) -> {
                Node node = db.createNode( Label.label( "L" ) );
                for ( int i = 0; i < 10; i++ )
                {
                    node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
                }
                tx.success();
            } );
        }

        File storeDir = coreGraphDatabase.storeDir();
        assertEventually( "pruning happened", () -> versionBy( storeDir, Math::min ), greaterThan( baseVersion ),
                5, SECONDS );

        // when
        cluster.addEdgeMemberWithIdAndRecordFormat( 42, HighLimit.NAME ).start();

        // then
        for ( final EdgeClusterMember edge : cluster.edgeMembers() )
        {
            assertEventually( "edge server available", () -> edge.database().isAvailable( 0 ), is( true ), 10,
                    SECONDS );
        }
    }

    private long versionBy( File storeDir, BinaryOperator<Long> operator )
    {
        File raftLogDir = new File( new File( storeDir, CLUSTER_STATE_DIRECTORY_NAME ), PHYSICAL_LOG_DIRECTORY_NAME );
        SortedMap<Long,File> logs =
                new FileNames( raftLogDir ).getAllFiles( new DefaultFileSystemAbstraction(), mock( Log.class ) );
        return logs.keySet().stream().reduce( operator ).orElseThrow( IllegalStateException::new );
    }

    private void executeOnLeaderWithRetry( Workload workload, Cluster cluster ) throws Exception
    {
        assertEventually( "Executed on leader", () -> {
            try
            {
                CoreGraphDatabase coreDB = cluster.awaitLeader( 5000 ).database();
                try ( Transaction tx = coreDB.beginTx() )
                {
                    workload.doWork( coreDB );
                    tx.success();
                    return true;
                }
            }
            catch ( AcquireLockTimeoutException | TransactionFailureException e )
            {
                // print the stack trace for diagnostic purposes, but retry as this is most likely a transient failure
                e.printStackTrace();
                return false;
            }
        }, is( true ), 30, SECONDS );
    }

    private interface Workload
    {
        void doWork( GraphDatabaseService database );
    }

    private void createData( GraphDatabaseService db )
    {
        for ( int i = 0; i < 10; i++ )
        {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
        }
    }
}
