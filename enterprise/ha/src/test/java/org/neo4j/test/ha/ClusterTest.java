/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.test.ha;

import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.client.Clusters;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.graphdb.factory.TestHighlyAvailableGraphDatabaseFactory;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.TransactionId;
import org.neo4j.kernel.impl.storemigration.LogFiles;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.Exceptions.rootCause;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterWithAdditionalArbiters;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesSlavesAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.provided;
import static org.neo4j.kernel.impl.pagecache.StandalonePageCacheFactory.createPageCache;
import static org.neo4j.kernel.impl.store.MetaDataStore.Position.LAST_TRANSACTION_COMMIT_TIMESTAMP;

public class ClusterTest
{
    @Rule
    public LoggerRule logging = new LoggerRule( Level.OFF );
    @Rule
    public TargetDirectory.TestDirectory testDirectory = TargetDirectory.testDirForTest( getClass() );

    @Test
    public void testCluster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "testCluster" ) )
                .withSharedConfig(
                    MapUtil.stringMap(
                            HaSettings.ha_server.name(), "localhost:6001-6005",
                            HaSettings.tx_push_factor.name(), "2" ) )
                .withProvider( clusterOfSize( 3 ) )
                .build();
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            long nodeId;
            HighlyAvailableGraphDatabase master = clusterManager.getDefaultCluster().getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                nodeId = node.getId();
                node.setProperty( "foo", "bar" );
                tx.success();
            }


            HighlyAvailableGraphDatabase slave = clusterManager.getDefaultCluster().getAnySlave();
            try ( Transaction transaction = slave.beginTx() )
            {
                Node node = slave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    public void testClusterWithHostnames() throws Throwable
    {
        Clusters.Cluster cluster = new Clusters.Cluster( "neo4j.ha" );
        for ( int i = 0; i < 3; i++ )
        {
            cluster.getMembers().add( new Clusters.Member( "localhost:" + (5001 + i), true ) );
        }

        final Clusters clusters = new Clusters();
        clusters.getClusters().add( cluster );

        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory(  "testCluster" ) )
                .withProvider( provided( clusters ) )
                .withSharedConfig( stringMap(
                        HaSettings.ha_server.name(), "localhost:6001-6005",
                        HaSettings.tx_push_factor.name(), "2" ) ).build();
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            long nodeId;
            HighlyAvailableGraphDatabase master = clusterManager.getDefaultCluster().getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                nodeId = node.getId();
                node.setProperty( "foo", "bar" );
                tx.success();
            }

            HighlyAvailableGraphDatabase anySlave = clusterManager.getDefaultCluster().getAnySlave();
            try ( Transaction ignore = anySlave.beginTx() )
            {
                Node node = anySlave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    public void testClusterWithWildcardIP() throws Throwable
    {
        Clusters.Cluster cluster = new Clusters.Cluster( "neo4j.ha" );
        for ( int i = 0; i < 3; i++ )
        {
            cluster.getMembers().add( new Clusters.Member( (5001 + i), true ) );
        }

        final Clusters clusters = new Clusters();
        clusters.getClusters().add( cluster );

        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory(  "testCluster" ) )
                .withProvider( provided( clusters ) )
                .withSharedConfig( stringMap(
                        HaSettings.ha_server.name(), "0.0.0.0:6001-6005",
                        HaSettings.tx_push_factor.name(), "2" ) ).build();
        try
        {
            clusterManager.start();

            clusterManager.getDefaultCluster().await( allSeesAllAsAvailable() );

            long nodeId;
            HighlyAvailableGraphDatabase master = clusterManager.getDefaultCluster().getMaster();
            try ( Transaction tx = master.beginTx() )
            {
                Node node = master.createNode();
                nodeId = node.getId();
                node.setProperty( "foo", "bar" );
                tx.success();
            }

            HighlyAvailableGraphDatabase anySlave = clusterManager.getDefaultCluster().getAnySlave();
            try ( Transaction ignore = anySlave.beginTx() )
            {
                Node node = anySlave.getNodeById( nodeId );
                assertThat( node.getProperty( "foo" ).toString(), CoreMatchers.equalTo( "bar" ) );
            }
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    public void testInstancesWithConflictingClusterPorts() throws Throwable
    {
        HighlyAvailableGraphDatabase first = null;
        try
        {
            String masterStoreDir =
                    testDirectory.directory( "testConflictingClusterPortsMaster" ).getAbsolutePath();
            first = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( masterStoreDir )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                    .newGraphDatabase();

            try
            {
                String slaveStoreDir =
                        testDirectory.directory( "testConflictingClusterPortsSlave" ).getAbsolutePath();
                HighlyAvailableGraphDatabase failed = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                        newHighlyAvailableDatabaseBuilder( slaveStoreDir )
                        .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                        .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                        .setConfig( ClusterSettings.server_id, "2" )
                        .setConfig( HaSettings.ha_server, "127.0.0.1:6667" )
                        .newGraphDatabase();
                failed.shutdown();
                fail("Should not start when ports conflict");
            }
            catch ( Exception e )
            {
                // good
            }
        }
        finally
        {
            if ( first != null )
            {
                first.shutdown();
            }
        }
    }

    @Test
    public void testInstancesWithConflictingHaPorts() throws Throwable
    {
        HighlyAvailableGraphDatabase first = null;
        try
        {
            String storeDir =
                    testDirectory.directory( "testConflictingHaPorts" ).getAbsolutePath();
             first = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                    newHighlyAvailableDatabaseBuilder( storeDir )
                    .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5001" )
                    .setConfig( ClusterSettings.server_id, "1" )
                    .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                    .newGraphDatabase();

            try
            {
                HighlyAvailableGraphDatabase failed = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                        newHighlyAvailableDatabaseBuilder( storeDir )
                        .setConfig( ClusterSettings.initial_hosts, "127.0.0.1:5001" )
                        .setConfig( ClusterSettings.cluster_server, "127.0.0.1:5002" )
                        .setConfig( ClusterSettings.server_id, "2" )
                        .setConfig( HaSettings.ha_server, "127.0.0.1:6666" )
                        .newGraphDatabase();
                failed.shutdown();
                fail( "Should not start when ports conflict" );
            }
            catch ( Exception e )
            {
                // good
            }
        }
        finally
        {
            if ( first != null )
            {
                first.shutdown();
            }
        }
    }

    @Test
    public void given4instanceClusterWhenMasterGoesDownThenElectNewMaster() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "4instances" ) )
                .withProvider( ClusterManager.clusterOfSize( 4 ) ).build();
        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( allSeesAllAsAvailable() );

            logging.getLogger().info( "STOPPING MASTER" );
            cluster.shutdown( cluster.getMaster() );
            logging.getLogger().info( "STOPPED MASTER" );

            cluster.await( ClusterManager.masterAvailable() );

            GraphDatabaseService master = cluster.getMaster();
            logging.getLogger().info( "CREATE NODE" );
            try ( Transaction tx = master.beginTx() )
            {
                master.createNode();
                logging.getLogger().info( "CREATED NODE" );
                tx.success();
            }

            logging.getLogger().info( "STOPPING CLUSTER" );
        }
        finally
        {
            clusterManager.safeShutdown();
        }
    }

    @Test
    public void givenEmptyHostListWhenClusterStartupThenFormClusterWithSingleInstance() throws Exception
    {
        HighlyAvailableGraphDatabase db = (HighlyAvailableGraphDatabase) new TestHighlyAvailableGraphDatabaseFactory().
                newHighlyAvailableDatabaseBuilder( testDirectory.directory(
                        "singleinstance" ).getAbsolutePath() ).
                setConfig( ClusterSettings.server_id, "1" ).
                setConfig( ClusterSettings.initial_hosts, "" ).
                newGraphDatabase();

        try
        {
            assertTrue( "Single instance cluster was not formed in time", db.isAvailable( 1_000 ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    public void givenClusterWhenMasterGoesDownAndTxIsRunningThenDontWaitToSwitch() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "waitfortx" ) )
                .withProvider( ClusterManager.clusterOfSize( 3 ) ).build();
        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( allSeesAllAsAvailable() );

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

            Transaction tx = slave.beginTx();
            // Do a little write operation so that all "write" aspects of this tx is initializes properly
            slave.createNode();

            // Shut down master while we're keeping this transaction open
            cluster.shutdown( cluster.getMaster() );

            cluster.await( masterAvailable() );
            cluster.await( masterSeesSlavesAsAvailable( 1 ) );
            // Ending up here means that we didn't wait for this transaction to complete

            tx.success();

            try
            {
                tx.close();
                fail( "Exception expected" );
            }
            catch ( Exception e )
            {
                assertThat( e, instanceOf( TransientTransactionFailureException.class ) );
                Throwable rootCause = rootCause( e );
                assertThat( rootCause, instanceOf( TransactionTerminatedException.class ) );
                assertThat( ((TransactionTerminatedException)rootCause).status(),
                        Matchers.<Status>equalTo( Status.General.DatabaseUnavailable ) );
            }
        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void lastTxCommitTimestampShouldGetInitializedOnSlaveIfNotPresent() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "lastTxTimestamp" ) )
                .withProvider( ClusterManager.clusterOfSize( 3 ) ).build();

        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( allSeesAllAsAvailable() );

            runSomeTransactions( cluster.getMaster() );
            cluster.sync();

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            File storeDir = new File( slave.getStoreDir() );
            ClusterManager.RepairKit slaveRepairKit = cluster.shutdown( slave );

            clearLastTransactionCommitTimestampField( storeDir );

            HighlyAvailableGraphDatabase repairedSlave = slaveRepairKit.repair();
            cluster.await( allSeesAllAsAvailable() );

            assertEquals( lastCommittedTxTimestamp( cluster.getMaster() ), lastCommittedTxTimestamp( repairedSlave ) );

        }
        finally
        {
            clusterManager.stop();
        }
    }

    @Test
    public void lastTxCommitTimestampShouldBeUnknownAfterStartIfNoFiledOrLogsPresent() throws Throwable
    {
        ClusterManager clusterManager = new ClusterManager.Builder( testDirectory.directory( "lastTxTimestamp" ) )
                .withProvider( ClusterManager.clusterOfSize( 3 ) ).build();

        try
        {
            clusterManager.start();
            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( allSeesAllAsAvailable() );

            runSomeTransactions( cluster.getMaster() );
            cluster.sync();

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            File storeDir = new File( slave.getStoreDir() );
            ClusterManager.RepairKit slaveRepairKit = cluster.shutdown( slave );

            clearLastTransactionCommitTimestampField( storeDir );
            deleteLogs( storeDir );

            HighlyAvailableGraphDatabase repairedSlave = slaveRepairKit.repair();
            cluster.await( allSeesAllAsAvailable() );

            assertEquals( TransactionIdStore.UNKNOWN_TX_COMMIT_TIMESTAMP, lastCommittedTxTimestamp( repairedSlave ) );
        }
        finally
        {
            clusterManager.stop();
        }
    }

    private static void deleteLogs( File storeDir )
    {
        for ( File file : storeDir.listFiles( LogFiles.FILENAME_FILTER ) )
        {
            FileUtils.deleteFile( file );
        }
    }

    private static void runSomeTransactions( HighlyAvailableGraphDatabase db )
    {
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction tx = db.beginTx() )
            {
                for ( int j = 0; j < 10; j++ )
                {
                    db.createNode();
                }
                tx.success();
            }
        }
    }

    private static void clearLastTransactionCommitTimestampField( File storeDir ) throws IOException
    {
        try ( PageCache pageCache = createPageCache( new DefaultFileSystemAbstraction() ) )
        {
            File neoStore = new File( storeDir, MetaDataStore.DEFAULT_NAME );
            MetaDataStore.setRecord( pageCache, neoStore, LAST_TRANSACTION_COMMIT_TIMESTAMP,
                    MetaDataStore.BASE_TX_COMMIT_TIMESTAMP );
        }
    }

    private static long lastCommittedTxTimestamp( HighlyAvailableGraphDatabase db )
    {
        DependencyResolver resolver = db.getDependencyResolver();
        MetaDataStore metaDataStore = resolver.resolveDependency( MetaDataStore.class );
        TransactionId txInfo = metaDataStore.getLastCommittedTransaction();
        return txInfo.commitTimestamp();
    }
}
