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
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.AbstractNeo4jTestCase;
import org.neo4j.server.Bootstrapper;
import org.neo4j.server.NeoServer;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.tooling.GlobalGraphOperations;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.Iterables.count;

@RunWith( Theories.class )
public class StoreUpgradeIT
{
    private static class Store
    {
        private final String resourceName;
        final long expectedNodeCount;

        private Store( String resourceName, long expectedNodeCount )
        {
            this.resourceName = resourceName;
            this.expectedNodeCount = expectedNodeCount;
        }

        public File prepareDirectory() throws IOException
        {
            File dir = AbstractNeo4jTestCase.unzip( StoreUpgradeIT.class, resourceName );
            new File( dir, "messages.log" ).delete(); // clear the log
            return dir;
        }
    }

    @DataPoints
    public static final Store[] stores = new Store[] {
            new Store( "/upgrade/0.A.1-db.zip", 1071 ),
            new Store( "0.A.1-db2.zip", 8 )
    };

    private long countAllNodes( GraphDatabaseService db )
    {
        return count( GlobalGraphOperations.at( db ).getAllNodes() );
    }

    @Test
    @Theory
    public void embeddedDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled(
            Store store ) throws IOException
    {
        File dir = store.prepareDirectory();

        GraphDatabaseFactory factory = new GraphDatabaseFactory();
        GraphDatabaseBuilder builder = factory.newEmbeddedDatabaseBuilder( dir.getAbsolutePath() );
        builder.setConfig( GraphDatabaseSettings.allow_store_upgrade, "true" );
        GraphDatabaseService db = builder.newGraphDatabase();

        System.out.println(dir);
        try ( Transaction ignore = db.beginTx() )
        {
            long count = countAllNodes( db );
            assertThat( count, is( store.expectedNodeCount ) );
        }
        finally
        {
            db.shutdown();
        }
    }

    @Test
    @Theory
    public void serverDatabaseShouldStartOnOlderStoreWhenUpgradeIsEnabled( Store store ) throws IOException
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
                GraphDatabaseAPI db = database.getGraph();

                try ( Transaction ignore = db.beginTx() )
                {
                    long count = countAllNodes( db );
                    assertThat( count, is( store.expectedNodeCount ) );
                }
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
    }
}
