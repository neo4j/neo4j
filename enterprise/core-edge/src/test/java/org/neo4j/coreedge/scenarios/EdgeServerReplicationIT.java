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

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.BinaryOperator;

import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.SharedDiscoveryService;
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
import org.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.neo4j.kernel.impl.store.format.standard.StandardV3_0;
import org.neo4j.logging.Log;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TargetDirectory;

import static java.io.File.pathSeparator;
import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.coreedge.raft.log.segmented.SegmentedRaftLog.SEGMENTED_LOG_DIRECTORY_NAME;
import static org.neo4j.coreedge.server.core.EnterpriseCoreEditionModule.CLUSTER_STATE_DIRECTORY_NAME;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class EdgeServerReplicationIT
{
    @Rule
    public final TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private Cluster cluster;

    @After
    public void shutdown() throws ExecutionException, InterruptedException
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldNotBeAbleToWriteToEdge() throws Exception
    {
        // given
        cluster = Cluster.start( dir.directory(), 3, 1, new SharedDiscoveryService() );

        GraphDatabaseService edgeDB = cluster.findAnEdgeServer();

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
        cluster = Cluster.start( dir.directory(), 3, 1, new SharedDiscoveryService() );

        // then
        for ( final EdgeGraphDatabase edgeGraphDatabase : cluster.edgeServers() )
        {
            ThrowingSupplier<Boolean,Exception> availability = () -> edgeGraphDatabase.isAvailable( 0 );
            assertEventually( "edge server becomes available", availability, is( true ), 10, SECONDS );
        }
    }

    @Test
    public void shouldEventuallyPullTransactionDownToAllEdgeServers() throws Exception
    {
        // given
        final SharedDiscoveryService discoveryServiceFactory = new SharedDiscoveryService();
        cluster = Cluster.start( dir.directory(), 3, 0, discoveryServiceFactory );
        int nodesBeforeEdgeServerStarts = 1;

        // when
        executeOnLeaderWithRetry( db -> {
            for ( int i = 0; i < nodesBeforeEdgeServerStarts; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
        } );

        cluster.addEdgeServerWithFileLocation( 0 );

        Set<EdgeGraphDatabase> edgeGraphDatabases = cluster.edgeServers();

        cluster.shutdownCoreServers();
        cluster = Cluster.start( dir.directory(), 3, 0, discoveryServiceFactory );

        // when
        executeOnLeaderWithRetry( db -> {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
        } );

        // then
        assertEquals( 1, edgeGraphDatabases.size() );

        for ( final GraphDatabaseService edgeDB : edgeGraphDatabases )
        {
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
        File edgeDatabaseStoreFileLocation = createExistingEdgeStore( dir.directory().getAbsolutePath() +
                pathSeparator + "edgeStore" );

        cluster = Cluster.start( dir.directory(), 3, 0, new SharedDiscoveryService() );

        executeOnLeaderWithRetry( db -> {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
        } );

        try
        {
            cluster.addEdgeServerWithFileLocation( edgeDatabaseStoreFileLocation );
            fail();
        }
        catch ( Throwable required )
        {
            // Lifecycle should throw exception, server should not start.
        }
    }

    @Test
    public void shouldThrowExceptionIfEdgeRecordFormatDiffersToCoreRecordFormat() throws Exception
    {
        // given
        String coreRecordFormat = HighLimit.NAME;
        String edgeRecordFormat = StandardV3_0.NAME;

        cluster = Cluster.start( dir.directory(), 3, 0, new SharedDiscoveryService(), coreRecordFormat );

        // when
        executeOnLeaderWithRetry( db -> {
            for ( int i = 0; i < 10; i++ )
            {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
            }
        } );

        try
        {
            cluster.addEdgeServerWithFileLocation( 0, edgeRecordFormat );
        }
        catch ( Exception e )
        {
            assertThat(e.getCause().getCause().getMessage(),
                    containsString("Failed to start database with copied store"));
        }
    }

    @Test
    public void shouldBeAbleToCopyStoresFromCoreToEdge() throws Exception
    {
        // given
        Map<String,String> params = stringMap(
                CoreEdgeClusterSettings.raft_log_rotation_size.name(), "1k",
                CoreEdgeClusterSettings.raft_log_pruning_frequency.name(), "500ms",
                CoreEdgeClusterSettings.state_machine_flush_window_size.name(), "1",
                CoreEdgeClusterSettings.raft_log_pruning_strategy.name(), "1 entries"
        );
        cluster = Cluster.start( dir.cleanDirectory( "db" ), 3, 0, new SharedDiscoveryService(),
                params,  emptyMap(), HighLimit.NAME );

        cluster.coreTx( ( db, tx ) -> {
            Node node = db.createNode( Label.label( "L" ) );
            for ( int i = 0; i < 10; i++ )
            {
                node.setProperty( "prop-" + i, "this is a quite long string to get to the log limit soonish" );
            }
            tx.success();
        } );

        long baseVersion = versionBy( new File( cluster.awaitLeader().getStoreDir() ), Math::max );

        CoreGraphDatabase coreGraphDatabase = null;
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

        File storeDir = new File( coreGraphDatabase.getStoreDir() );
        assertEventually( "pruning happened", () -> versionBy( storeDir, Math::min ), greaterThan( baseVersion ), 1, SECONDS );

        // when
        cluster.addEdgeServerWithFileLocation( 42, HighLimit.NAME );

        // then
        for ( final EdgeGraphDatabase edge : cluster.edgeServers() )
        {
            assertEventually( "edge server available", () -> edge.isAvailable( 0 ), is( true ), 10, SECONDS );
        }
    }

    private long versionBy( File storeDir, BinaryOperator<Long> operator )
    {
        File raftLogDir = new File( new File( storeDir, CLUSTER_STATE_DIRECTORY_NAME ), SEGMENTED_LOG_DIRECTORY_NAME );
        SortedMap<Long,File> logs =
                new FileNames( raftLogDir ).getAllFiles( new DefaultFileSystemAbstraction(), mock( Log.class ) );
        return logs.keySet().stream().reduce( operator ).get();
    }

    private File createExistingEdgeStore( String path )
    {
        File dir = new File( path );
        dir.mkdirs();

        GraphDatabaseService db =
                new TestGraphDatabaseFactory().newEmbeddedDatabase( Cluster.edgeServerStoreDirectory( dir, 1966 ) );

        try ( Transaction tx = db.beginTx() )
        {
            db.createNode();
            tx.success();
        }

        db.shutdown();

        return dir;
    }

    private GraphDatabaseService executeOnLeaderWithRetry( Workload workload ) throws TimeoutException
    {
        CoreGraphDatabase coreDB;
        while ( true )
        {
            coreDB = cluster.awaitLeader( 5000 );
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
