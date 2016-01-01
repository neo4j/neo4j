/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static org.neo4j.consistency.store.StoreAssertions.assertConsistentStore;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;

@RunWith( Theories.class )
public class StoreUpgradeIntegrationTest
{
    @DataPoints
    public static final Store[] stores = new Store[] {
            new Store( "/upgrade/0.A.1-db.zip", 1071, 0, 18 ),
            new Store( "0.A.1-db2.zip", 8, 1, 11 ),
            new Store( "0.A.0-db.zip", 4, 0, 4 ),
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
        HighlyAvailableGraphDatabase master, slave;
        try
        {
            cluster.await( allSeesAllAsAvailable() );

            master = cluster.getMaster();
            checkInstance( store, master );
            slave = cluster.getAnySlave();
            checkInstance( store, slave );
        }
        finally
        {
            clusterManager.shutdown();
        }

        assertConsistentStore( new File( master.getStoreDir() ) );
        assertConsistentStore( new File( slave.getStoreDir() ) );
    }

    private static class Store
    {
        private final String resourceName;
        final long expectedNodeCount;
        final long lastTxId;
        final long expectedIndexCount;


        private Store( String resourceName, long expectedNodeCount, long expectedIndexCount, long lastTxId )
        {
            this.resourceName = resourceName;
            this.expectedNodeCount = expectedNodeCount;
            this.expectedIndexCount = expectedIndexCount;
            this.lastTxId = lastTxId;
        }

        public File prepareDirectory() throws IOException
        {
            File dir = AbstractNeo4jTestCase.unzip( StoreUpgradeIntegrationTest.class, resourceName );
            new File( dir, "messages.log" ).delete(); // clear the log
            return dir;
        }
    }

    private static void checkInstance( Store store, GraphDatabaseAPI db )
    {
        try ( Transaction ignored = db.beginTx() )
        {
            // count nodes
            long nodeCount = count( GlobalGraphOperations.at( db ).getAllNodes() );
            assertThat( nodeCount, is( store.expectedNodeCount ) );

            // count indexes
            long indexCount = count( db.schema().getIndexes() );
            assertThat( indexCount, is( store.expectedIndexCount ) );

            // check last committed tx
            long lastCommittedTxId = db.getDependencyResolver()
                    .resolveDependency( NeoStoreXaDataSource.class )
                    .getLastCommittedTxId();

            assertThat( lastCommittedTxId, is( store.lastTxId ) );
        }
    }
}
