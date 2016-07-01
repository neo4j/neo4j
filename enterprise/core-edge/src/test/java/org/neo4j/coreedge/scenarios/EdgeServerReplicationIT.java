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

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.CoreServer;
import org.neo4j.coreedge.discovery.EdgeServer;
import org.neo4j.coreedge.raft.log.segmented.FileNames;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.core.CoreGraphDatabase;
import org.neo4j.coreedge.server.edge.EdgeGraphDatabase;
import org.neo4j.function.ThrowingSupplier;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.kernel.lifecycle.LifecycleException;
import org.neo4j.logging.Log;
import org.neo4j.test.coreedge.ClusterRule;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog.SEGMENTED_LOG_DIRECTORY_NAME;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.TIME;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class EdgeServerReplicationIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule( getClass() ).withNumberOfCoreServers( 3 ).withNumberOfEdgeServers( 1 );

    @Test
    public void shouldNotBeAbleToWriteToEdge() throws Exception
    {
        // given
        Cluster cluster = clusterRule.startCluster();

        EdgeGraphDatabase edgeDB = cluster.findAnEdgeServer().database();

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
        for ( final EdgeServer edgeServer : cluster.edgeServers() )
        {
            ThrowingSupplier<Boolean,Exception> availability = () -> edgeServer.database().isAvailable( 0 );
            assertEventually( "edge server becomes available", availability, is( true ), 10, SECONDS );
        }
    }

    @Test
    public void shouldEventuallyPullTransactionDownToAllEdgeServers() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfEdgeServers( 0 ).startCluster();
        int nodesBeforeEdgeServerStarts = 1;

        // when
        executeOnLeaderWithRetry( db -> {
            for ( int i = 0; i < nodesBeforeEdgeServerStarts; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
        }, cluster );

        cluster.addEdgeServerWithId( 0 ).start();

        // when
        executeOnLeaderWithRetry( db -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
        }, cluster );

        // then
        for ( final EdgeServer server : cluster.edgeServers() )
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
    public void shouldShutdownRatherThanPullUpdatesFromCoreServerWithDifferentStoreIfServerHasData() throws Exception
    {
        Cluster cluster = clusterRule.withNumberOfEdgeServers( 0 ).startCluster();

        executeOnLeaderWithRetry( db -> {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
        }, cluster );

        EdgeServer edgeServer = cluster.addEdgeServerWithId( 4 );
        putSomeDataWithDifferentStoreId( edgeServer.storeDir(), cluster.getCoreServerById( 0 ).storeDir() );

        try
        {
            edgeServer.start();
            fail( "Should have failed to start" );
        }
        catch ( LifecycleException required )
        {
            // Lifecycle should throw exception, server should not start.
            assertThat( required.getCause(), instanceOf( LifecycleException.class ) );
            assertThat( required.getCause().getCause(), instanceOf( IllegalStateException.class ) );
            assertThat( required.getCause().getCause().getMessage(),
                    equalTo( "Local database is not empty cannot copy store" ) );
        }
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
    public void shouldThrowExceptionIfEdgeRecordFormatDiffersToCoreRecordFormat() throws Exception
    {
        // given
        Cluster cluster = clusterRule.withNumberOfEdgeServers( 0 ).withRecordFormat( HighLimit.NAME ).startCluster();

        // when
        executeOnLeaderWithRetry( db -> {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
        }, cluster );

        try
        {
            cluster.addEdgeServerWithIdAndRecordFormat( 0, StandardV3_0.NAME );
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
        Cluster cluster = clusterRule.withNumberOfEdgeServers( 0 ).withSharedCoreParams( params )
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

        CoreServer coreGraphDatabase = null;
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
        assertEventually( "pruning happened", () -> versionBy( storeDir, Math::min ), greaterThan( baseVersion ), 1,
                SECONDS );

        // when
        cluster.addEdgeServerWithIdAndRecordFormat( 42, HighLimit.NAME ).start();

        // then
        for ( final EdgeServer edge : cluster.edgeServers() )
        {
            assertEventually( "edge server available", () -> edge.database().isAvailable( 0 ), is( true ), 10,
                    SECONDS );
        }
    }

    private long versionBy( File storeDir, BinaryOperator<Long> operator )
    {
        File raftLogDir = new File( new File( storeDir, CLUSTER_STATE_DIRECTORY_NAME ), SEGMENTED_LOG_DIRECTORY_NAME );
        SortedMap<Long,File> logs =
                new FileNames( raftLogDir ).getAllFiles( new DefaultFileSystemAbstraction(), mock( Log.class ) );
        return logs.keySet().stream().reduce( operator ).orElseThrow( IllegalStateException::new );
    }

    private GraphDatabaseService executeOnLeaderWithRetry( Workload workload, Cluster cluster ) throws TimeoutException
    {
        CoreGraphDatabase coreDB;
        while ( true )
        {
            coreDB = cluster.awaitLeader( 5000 ).database();
            try ( Transaction tx = coreDB.beginTx() )
            {
                workload.doWork( coreDB );
                tx.success();
                break;
            }
            catch ( TransactionFailureException e )
            {
                // print the stack trace for diagnostic purposes, but retry as this is most likely a transient failure
                e.printStackTrace();
            }
        }
        return coreDB;
    }

    private interface Workload
    {
        void doWork( GraphDatabaseService database );
    }
}
