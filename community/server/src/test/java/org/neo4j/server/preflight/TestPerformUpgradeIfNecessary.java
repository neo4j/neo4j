/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InOrder;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader.Monitor;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.Unzip;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.io.fs.FileUtils.copyRecursively;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

public class TestPerformUpgradeIfNecessary
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    private File homeDir;
    private File storeDir;
    private File confDir;
    private File neo4jProperties;

    @Before
    public void setup()
    {
        homeDir = testDir.directory( "home" );
        storeDir = new File( new File( homeDir, "data" ), "graph.db" );
        confDir = new File( homeDir, "conf" );
        neo4jProperties = new File( confDir, "neo4j.properties" );
    }

    @Test
    public void shouldExitImmediatelyIfStoreIsAlreadyAtLatestVersion() throws IOException
    {
        Config serverConfig = buildProperties( false );
        GraphDatabaseBuilder builder =
                new TestGraphDatabaseFactory().newEmbeddedDatabaseBuilder( storeDir );
        builder.newGraphDatabase().shutdown();

        Monitor monitor = mock( Monitor.class );
        PerformUpgradeIfNecessary upgrader = new PerformUpgradeIfNecessary( serverConfig,
                loadNeo4jProperties(), NullLogProvider.getInstance(), monitor );

        boolean exit = upgrader.run();

        assertEquals( true, exit );

        verifyNoMoreInteractions( monitor );
    }

    @Test
    public void shouldGiveHelpfulMessageIfAutoUpgradeParameterNotSet() throws IOException
    {
        Config serverProperties = buildProperties( false );
        prepareSampleLegacyDatabase( storeDir );

        Monitor monitor = mock( Monitor.class );
        PerformUpgradeIfNecessary upgrader = new PerformUpgradeIfNecessary( serverProperties,
                loadNeo4jProperties(), NullLogProvider.getInstance(), monitor );

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
                loadNeo4jProperties(), NullLogProvider.getInstance(), monitor );

        boolean exit = upgrader.run();

        assertEquals( true, exit );

        verifyNoMoreInteractions( monitor );
    }

    @Test
    public void shouldUpgradeDatabase() throws IOException
    {
        // Given
        Config serverConfig = buildProperties( true );
        prepareSampleLegacyDatabase( storeDir );

        Monitor monitor = mock( Monitor.class );
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        AssertableLogProvider assertableLogProvider = new AssertableLogProvider();
        LogProvider logProvider = new DuplicatingLogProvider( FormattedLogProvider.toOutputStream( outputStream ),
                assertableLogProvider );

        PerformUpgradeIfNecessary upgrader = new PerformUpgradeIfNecessary( serverConfig,
                loadNeo4jProperties(), logProvider, monitor );

        // When
        boolean success = upgrader.run();

        // Then
        if ( !success )
        {
            System.out.write( outputStream.toByteArray() );
            fail();
        }

        InOrder order = inOrder( monitor );
        order.verify( monitor, times( 1 ) ).migrationNeeded();
        order.verify( monitor, times( 1 ) ).migrationCompleted();
        order.verifyNoMoreInteractions();

        assertableLogProvider.assertContainsMessageContaining( "Migration completed" );
    }

    private Config buildProperties( boolean allowStoreUpgrade ) throws IOException
    {
        FileUtils.deleteRecursively( homeDir );
        assertTrue( confDir.mkdirs() );

        Properties databaseProperties = new Properties();
        if ( allowStoreUpgrade )
        {
            databaseProperties.setProperty( GraphDatabaseSettings.allow_store_upgrade.name(), "true" );
        }

        databaseProperties.store( new FileWriter( neo4jProperties.getAbsolutePath() ), null );

        Config serverProperties = new Config( MapUtil.stringMap(
                Configurator.DATABASE_LOCATION_PROPERTY_KEY, storeDir.getPath(),
                Configurator.DB_TUNING_PROPERTY_FILE_KEY, neo4jProperties.getAbsolutePath() ) );

        return serverProperties;
    }

    private Map<String,String> loadNeo4jProperties() throws IOException
    {
        return MapUtil.load( new File( neo4jProperties.getAbsolutePath() ) );
    }

    private void prepareSampleLegacyDatabase( File workingDirectory ) throws IOException
    {
        File resourceDirectory = Unzip.unzip( TestPerformUpgradeIfNecessary.class, "exampledb.zip" );
        deleteRecursively( workingDirectory );
        assertTrue( workingDirectory.mkdirs() );
        copyRecursively( resourceDirectory, workingDirectory );
    }
}
