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
package org.neo4j.server.database;

import java.io.File;
import java.util.Map;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.Functions;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.ConfigDatabase;
import org.neo4j.test.BufferingLogging;
import org.neo4j.test.TargetDirectory;

import static junit.framework.Assert.assertEquals;
import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import static org.neo4j.graphdb.factory.GraphDatabaseSettings.cache_type;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.store_dir;
import static org.neo4j.helpers.collection.Iterables.count;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.NeoServerSettings.config_db_path;
import static org.neo4j.server.NeoServerSettings.legacy_db_config;
import static org.neo4j.server.NeoServerSettings.legacy_db_location;
import static org.neo4j.server.database.LifecycleManagingDatabase.EMBEDDED;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class DatabaseHostingIT
{
    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );

    private LifeSupport life = new LifeSupport(  );
    private final Logging logging = new BufferingLogging();
    private DatabaseRegistry registry;
    private DatabaseHosting host;
    private ConfigDatabase configDb;
    private Config serverConfig;

    @Test
    public void shouldLoadDatabasesFromConfigDb() throws Throwable
    {
        // Given
        createHosting();

        // When
        host.newDatabase( "somekey", "single", DatabaseHosting.Mode.EXTERNAL, new Config(stringMap( store_dir.name(), testDir.absolutePath() + "/nomatter")) );

        // Then
        assertTrue(registry.contains( "somekey" ));
        assertTrue( new File( testDir.directory(), "nomatter" ).exists());

        // And when I..
        restartHosting();

        // Then the new database should have been reloaded from the config db
        assertTrue( registry.contains( "somekey" ) );
    }

    @Test
    public void shouldDeleteManagedDatabases() throws Exception
    {
        // Given
        createHosting();
        host.newDatabase( "somekey", "single", DatabaseHosting.Mode.MANAGED, new Config(stringMap( store_dir.name(), testDir.absolutePath() + "/nomatter")) );

        // When
        host.dropDatabase( "somekey" );

        // Then
        assertFalse( registry.contains( "somekey" ) );
        assertFalse( new File( testDir.directory(), "nomatter" ).exists() );

        // And when I..
        restartHosting();

        // Then the new database should have been reloaded from the config db
        assertFalse( registry.contains( "somekey" ) );
        assertFalse( new File( testDir.directory(), "nomatter" ).exists());
    }

    @Test
    public void shouldKeepFilesForExternalDatabases() throws Exception
    {
        // Given
        createHosting();
        host.newDatabase( "somekey", "single", DatabaseHosting.Mode.EXTERNAL, new Config(stringMap( store_dir.name(), testDir.absolutePath() + "/nomatter")) );

        // When
        host.dropDatabase( "somekey" );

        // Then
        assertFalse( registry.contains( "somekey" ) );
        assertTrue( new File( testDir.directory(), "nomatter" ).exists() );

        // And when I..
        restartHosting();

        // Then the new database should have been reloaded from the config db
        assertFalse( registry.contains( "somekey" ) );
        assertTrue( new File( testDir.directory(), "nomatter" ).exists());
    }

    @Test
    public void shouldCreateADatabaseNamedDBOnFreshSlateStartup() throws Exception
    {
        // Given
        createHosting();

        // Then
        DatabaseDefinition db = single( configDb.listDatabases() );
        assertEquals( "db", db.key() );
        assertEquals( new File(testDir.directory(), "data/graph.db").getAbsolutePath(), db.path().getAbsolutePath() );

        // But when
        host.dropDatabase( "db" );
        restartHosting();

        // Then it should not get created again
        assertThat(count( configDb.listDatabases() ), equalTo(0l));
    }

    @Test
    public void legacyDbConfigOverridesRuntimeConfig() throws Exception
    {
        // Given
        Map<String,String> cfg = serverConfig.getParams();
        File propertiesFile = new File( testDir.directory(), "cfg.properties" );
        cfg.put( legacy_db_config.name(), propertiesFile.getAbsolutePath() );
        serverConfig.applyChanges( cfg );

        MapUtil.store(stringMap( cache_type.name(), "none" ), propertiesFile);

        // When
        restartHosting();

        // Then
        assertEquals( "none", single( configDb.listDatabases() ).config().get( cache_type ) );
    }

    @Test
    public void shouldNotClearLegacyRuntimeConfigIfNoConfigFileExists() throws Exception
    {
        // Given
        Map<String,String> cfg = serverConfig.getParams();
        File propertiesFile = new File( testDir.directory(), "cfg.properties" );
        cfg.put( legacy_db_config.name(), propertiesFile.getAbsolutePath() );
        serverConfig.applyChanges( cfg );

        createHosting();

        host.reconfigureDatabase( "db", new Config( stringMap( cache_type.name(), "weak" ) ) );

        // When
        restartHosting();

        // Then
        assertEquals( "weak", single( configDb.listDatabases() ).config().get( cache_type ));
    }

    @Test
    public void shouldBeAbleToSetDatabaseProvider() throws Exception
    {
        // Given
        createHosting();
        host.newDatabase( "mydb", "single", DatabaseHosting.Mode.EXTERNAL, new Config(stringMap( store_dir.name(), testDir.absolutePath() + "/nomatter")) );

        // When
        host.changeDatabaseProvider( "mydb", "someOtherProvider" );

        // Then
        assertEquals("someOtherProvider", configDb.getDatabase( "mydb" ).provider());
    }

    @Test
    public void dbWithUnknownProviderShouldNotStopServerFromStarting() throws Exception
    {
        // Given
        createHosting();
        configDb.newDatabase( "lol", "doesntexist", DatabaseHosting.Mode.EXTERNAL, new Config(stringMap( store_dir.name(), testDir.absolutePath() + "/nomatter")) );

        // When
        restartHosting();

        // Then
        assertThat( logging.toString(), CoreMatchers.containsString( "Unable to start database 'lol', " +
                "because there is no database provider called 'doesntexist', which this database has been configured " +
                "to use." ) );
    }

    private void restartHosting()
    {
        life.shutdown();
        life = new LifeSupport(  );
        createHosting();
    }

    private void createHosting()
    {
        registry = life.add( registry() );
        configDb = life.add(new ConfigDatabase( registry, serverConfig ));
        host = life.add(new DatabaseHosting( registry, configDb, logging.getMessagesLog( DatabaseHosting.class ) ));
        life.start();
    }

    @Before
    public void setup()
    {
        serverConfig = new Config( stringMap(
                config_db_path.name(), new File(testDir.directory(), "/__config__.db" ).getAbsolutePath(),
                legacy_db_location.name(), new File(testDir.directory(), "/data/graph.db" ).getAbsolutePath()));
    }

    @After
    public void shutdown()
    {
        life.shutdown();
        life = new LifeSupport();
    }

    private DatabaseRegistry registry()
    {
        DatabaseRegistry registry = new DatabaseRegistry( Functions.<Config,InternalAbstractGraphDatabase.Dependencies>constant(GraphDatabaseDependencies.newDependencies().logging(logging)) );
        registry.addProvider( "single", lifecycleManagingDatabase( EMBEDDED ));
        registry.addProvider( "someOtherProvider", lifecycleManagingDatabase( EMBEDDED ));
        return registry;
    }

}
