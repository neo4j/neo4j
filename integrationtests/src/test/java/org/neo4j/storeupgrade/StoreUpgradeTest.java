/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storeupgrade;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import org.neo4j.consistency.checking.full.ConsistencyCheckIncompleteException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.transaction.xaframework.LogFileInformation;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

@RunWith(Enclosed.class)
public class StoreUpgradeTest
{
    /*
     * TODO 2.2-future
     *
     * This test claims that we can upgrade from 1.9/2.0 to 2.2, this does not work when running HA since we cannot
     * parse the 1.9/2.0 log files since we changed the format of such files. It does work when using a single instance
     * since we didn't change the storage format and in such case we do not read the old logs.
     *
     * We need to understand if this is a feature we need to have in 2.2 or not.
     */

    @RunWith(Theories.class)
    public static class StoreUpgradeSingleInstanceTest
    {
        // NOTE: the zip files must contain the database files and NOT the graph.db folder itself!!!
        @DataPoints
        public static final Store[] stores = new Store[]{
                new Store( "/upgrade/0.A.1-db.zip", 1071, 18 ),
                new Store( "0.A.1-db2.zip", 8, 11 ),
                new Store( "0.A.3-empty.zip", 0, 1 ), // 2.1.3
                new Store( "0.A.3-data.zip", 2, 6 ), // 2.1.3
        };

        @Test
        @Theory
        public void embeddedDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled( Store store )
                throws IOException, ConsistencyCheckIncompleteException
        {
            File dir = store.prepareDirectory();

            GraphDatabaseFactory factory = new GraphDatabaseFactory();
            GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( dir.getAbsolutePath() );
            builder.setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" );
            GraphDatabaseService db = builder.newGraphDatabase();

            try
            {
                checkInstance( store, (GraphDatabaseAPI) db );
            }
            finally
            {
                db.shutdown();
            }
            assertConsistentStore( dir );
        }

        @Test
        @Theory
        public void serverDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled( Store store )
                throws IOException, ConsistencyCheckIncompleteException

        {
            File dir = store.prepareDirectory();

            File configFile = new File( dir, "neo4j.properties" );
            Properties props = new Properties();
            props.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY, dir.getAbsolutePath() );
            props.setProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, configFile.getAbsolutePath() );
            props.setProperty( GraphDatabaseSettings.allow_store_upgrade.name(), "true" );
            props.store( new FileWriter( configFile ), "" );

            try
            {
                System.setProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY, configFile.getAbsolutePath() );

                Bootstrapper bootstrapper = Bootstrapper.loadMostDerivedBootstrapper();
                bootstrapper.start();

                try
                {
                    NeoServer server = bootstrapper.getServer();
                    Database database = server.getDatabase();
                    checkInstance( store, database.getGraph() );
                }
                finally
                {
                    bootstrapper.stop();
                }
            }
            finally
            {
                System.clearProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY );
            }

            assertConsistentStore( dir );
        }
    }

    @RunWith(Theories.class)
    public static class StoreUpgradeHATest
    {
        // NOTE: the zip files must contain the database files and NOT the graph.db folder itself!!!
        @DataPoints
        public static final Store[] stores = new Store[]{
                new Store( "0.A.3-empty.zip", 0, 1 ), // 2.1.3
                new Store( "0.A.3-data.zip", 2, 6 ), // 2.1.3
        };

        @Test
        @Theory
        public void migratingOlderDataAndThanStartAClusterUsingTheNewerDataShouldWork( Store store ) throws Throwable
        {
            // migrate the store using a single instance
            File dir = store.prepareDirectory();
            GraphDatabaseFactory factory = new GraphDatabaseFactory();
            GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( dir.getAbsolutePath() );
            builder.setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" );
            GraphDatabaseService db = builder.newGraphDatabase();
            try
            {
                checkInstance( store, (GraphDatabaseAPI) db );
            }
            finally
            {
                db.shutdown();
            }

            assertConsistentStore( dir );

            // start the cluster with the db migrated from the old instance
            File haDir = new File( dir.getParentFile(), "ha-stuff" );
            FileUtils.deleteRecursively( haDir );
            ClusterManager clusterManager = new ClusterManager(
                    new ClusterManager.Builder( haDir ).withSeedDir( dir ).withProvider( clusterOfSize( 2 ) )
            );

            clusterManager.start();

            ClusterManager.ManagedCluster cluster = clusterManager.getDefaultCluster();
            cluster.await( allSeesAllAsAvailable() );

            HighlyAvailableGraphDatabase master = cluster.getMaster();
            checkInstance( store, master );
            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            checkInstance( store, slave );

            clusterManager.shutdown();

            assertConsistentStore( new File( master.getStoreDir() ) );
            assertConsistentStore( new File( slave.getStoreDir() ) );
        }
    }

    private static class Store
    {
        private final String resourceName;
        final long expectedNodeCount;
        final long lastTxId;

        private Store( String resourceName, long expectedNodeCount, long lastTxId )
        {
            this.resourceName = resourceName;
            this.expectedNodeCount = expectedNodeCount;
            this.lastTxId = lastTxId;
        }

        public File prepareDirectory() throws IOException
        {
            File dir = AbstractNeo4jTestCase.unzip( StoreUpgradeTest.class, resourceName );
            new File( dir, "messages.log" ).delete(); // clear the log
            return dir;
        }
    }

    private static void checkInstance( Store store, GraphDatabaseAPI db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            long count = count( GlobalGraphOperations.at( db ).getAllNodes() );
            assertThat( count, is( store.expectedNodeCount ) );

            long lastCommittedTxId = db.getDependencyResolver()
                    .resolveDependency( NeoStoreXaDataSource.class )
                    .getDependencyResolver()
                    .resolveDependency( LogFileInformation.class )
                    .getLastCommittedTxId();
            assertEquals( store.lastTxId, lastCommittedTxId );
        }
    }
}
