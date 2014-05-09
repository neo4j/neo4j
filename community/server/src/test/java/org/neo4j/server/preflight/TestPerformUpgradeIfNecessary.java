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
package org.neo4j.server.preflight;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;
import org.mockito.InOrder;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.Monitor;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.MapBasedConfiguration;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.Unzip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.kernel.impl.util.FileUtils.copyRecursively;
import static org.neo4j.kernel.impl.util.FileUtils.deleteRecursively;
import static org.neo4j.kernel.logging.DevNullLoggingService.DEV_NULL;

public class TestPerformUpgradeIfNecessary
{
    public static final File HOME_DIRECTORY = TargetDirectory.forTest( TestPerformUpgradeIfNecessary.class )
            .cleanDirectory( "home" );
    public static final File STORE_DIRECTORY = new File( new File( HOME_DIRECTORY, "data" ), "graph.db" );

    @Test
    public void shouldExitImmediatelyIfStoreIsAlreadyAtLatestVersion() throws IOException
    {
        Configuration serverConfig = buildProperties( false );
        new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( STORE_DIRECTORY.getPath() ).newGraphDatabase().shutdown();

        Monitor monitor = mock( Monitor.class );
        PerformUpgradeIfNecessary upgrader = new PerformUpgradeIfNecessary( serverConfig,
        		loadNeo4jProperties(), DEV_NULL, monitor );

        boolean exit = upgrader.run();

        assertEquals( true, exit );

        verifyNoMoreInteractions( monitor );
    }

    @Test
    public void shouldGiveHelpfulMessageIfAutoUpgradeParameterNotSet() throws IOException
    {
        Configuration serverProperties = buildProperties( false );
        prepareSampleLegacyDatabase( STORE_DIRECTORY );

        Monitor monitor = mock( Monitor.class );
        PerformUpgradeIfNecessary upgrader = new PerformUpgradeIfNecessary( serverProperties,
        		loadNeo4jProperties(), DEV_NULL, monitor );

        boolean exit = upgrader.run();

        assertEquals( false, exit );

        verify( monitor, times( 1 ) ).migrationNeeded();
        verify( monitor, times( 1 ) ).migrationNotAllowed();
        verifyNoMoreInteractions( monitor );
    }

    @Test
    public void shouldExitCleanlyIfDatabaseMissingSoThatDatabaseCreationIsLeftToMainProcess() throws IOException
    {
        Monitor monitor = mock( Monitor.class );
        PerformUpgradeIfNecessary upgrader = new PerformUpgradeIfNecessary( buildProperties( true ),
        		loadNeo4jProperties(), DEV_NULL, monitor );

        boolean exit = upgrader.run();

        assertEquals( true, exit );

        verifyNoMoreInteractions( monitor );
    }

    @Test
    public void shouldUpgradeDatabase() throws IOException
    {
        Configuration serverConfig = buildProperties( true );
        prepareSampleLegacyDatabase( STORE_DIRECTORY );

        Monitor monitor = mock( Monitor.class );
        PerformUpgradeIfNecessary upgrader = new PerformUpgradeIfNecessary( serverConfig,
        		loadNeo4jProperties(), DEV_NULL, monitor );

        boolean exit = upgrader.run();

        assertEquals( true, exit );

        InOrder order = inOrder( monitor );
        order.verify( monitor, times( 1 ) ).migrationNeeded();
        order.verify( monitor, times( 1 ) ).migrationCompleted();
        order.verifyNoMoreInteractions();
    }

    private Configuration buildProperties(boolean allowStoreUpgrade) throws IOException
    {
        FileUtils.deleteRecursively( HOME_DIRECTORY );
        File confDir = new File( HOME_DIRECTORY, "conf" );
        confDir.mkdirs();

        Properties databaseProperties = new Properties();
        if (allowStoreUpgrade)
        {
            databaseProperties.setProperty( GraphDatabaseSettings.allow_store_upgrade.name(), "true" );
        }
        String databasePropertiesFileName = new File( confDir, "neo4j.properties" ).getAbsolutePath();
        databaseProperties.store( new FileWriter( databasePropertiesFileName ), null );

        Configuration serverProperties = new MapBasedConfiguration();
        serverProperties.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY, STORE_DIRECTORY.getPath() );
        serverProperties.setProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, databasePropertiesFileName );

        return serverProperties;
    }

    private Map<String,String> loadNeo4jProperties() throws IOException
    {
        String databasePropertiesFileName = new File( new File( HOME_DIRECTORY, "conf" ),
                "neo4j.properties" ).getAbsolutePath();
        return MapUtil.load(new File(databasePropertiesFileName));
    }

    public static void prepareSampleLegacyDatabase( File workingDirectory ) throws IOException
    {
        File resourceDirectory = findOldFormatStoreDirectory();

        deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );

        copyRecursively( resourceDirectory, workingDirectory );
    }

    public static File findOldFormatStoreDirectory() throws IOException
    {
        return Unzip.unzip( TestPerformUpgradeIfNecessary.class, "exampledb.zip" );
    }

    private String dots( int count )
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < count; i++) {
            builder.append( "." );
        }
        return builder.toString();
    }
}
