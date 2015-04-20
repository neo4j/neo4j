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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Properties;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.recovery.StoreRecoverer;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.TestGraphDatabaseFactory;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.neo4j.kernel.impl.recovery.TestStoreRecoverer.createLogFileForNextVersionWithSomeDataInIt;

public class TestPerformRecoveryIfNecessary {

    @Rule
    public TargetDirectory.TestDirectory testDir = TargetDirectory.testDirForTest( getClass() );
    public String homeDirectory;
    public String storeDirectory;

    private static final String LINEBREAK = System.getProperty("line.separator");

    @Before
    public void createDirs()
    {
        homeDirectory = testDir.directory().getAbsolutePath();
        storeDirectory = new File(homeDirectory, "data" + File.separator + "graph.db").getAbsolutePath();
    }

    @Test
    public void shouldNotDoAnythingIfNoDBPresent() throws Exception
    {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Config config = buildProperties();
        PerformRecoveryIfNecessary task = new PerformRecoveryIfNecessary( config, new HashMap<String, String>(),
                new PrintStream( outputStream ), DevNullLoggingService.DEV_NULL );
        
        assertThat( "Recovery task runs successfully.", task.run(), is( true ) );
        assertThat( "No database should have been created.", new File( storeDirectory ).exists(), is( false ) );
        assertThat( "Recovery task should not print anything.", outputStream.toString(), is( "" ) );
    }

    @Test
    public void doesNotPrintAnythingIfDatabaseWasCorrectlyShutdown() throws Exception
    {
        // Given
        Config config = buildProperties();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDirectory ).shutdown();
        
        PerformRecoveryIfNecessary task = new PerformRecoveryIfNecessary( config, new HashMap<String, String>(),
                new PrintStream( outputStream ), DevNullLoggingService.DEV_NULL );
        
        assertThat( "Recovery task should run successfully.", task.run(), is( true ) );
        assertThat( "Database should exist.", new File( storeDirectory ).exists(), is( true ) );
        assertThat( "Recovery should not print anything.", outputStream.toString(), is( "" ) );
    }
    
    @Test
    public void shouldPerformRecoveryIfNecessary() throws Exception
    {
        // Given
        TestLogging logging = new TestLogging();
        StoreRecoverer recoverer = new StoreRecoverer();
        Config config = buildProperties();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        new TestGraphDatabaseFactory().newEmbeddedDatabase( storeDirectory ).shutdown();
        // Make this look incorrectly shut down
        createLogFileForNextVersionWithSomeDataInIt( new File( storeDirectory ), new DefaultFileSystemAbstraction() );

        assertThat("Store should need recovery", recoverer.recoveryNeededAt(new File( storeDirectory )), is(true));

        // Run recovery
        PerformRecoveryIfNecessary task = new PerformRecoveryIfNecessary(config, new HashMap<String,String>(), new PrintStream(outputStream), logging );
        assertThat("Recovery task should run successfully.", task.run(), is(true));
        assertThat("Database should exist.", new File( storeDirectory ).exists(), is(true));
        assertThat("Recovery should print status message.", outputStream.toString(), is("Detected incorrectly shut down database, performing recovery.." + LINEBREAK));
        assertThat("Store should be recovered", recoverer.recoveryNeededAt( new File( storeDirectory )), is(false));
	}

    @Test
    public void shouldNotPerformRecoveryIfNoNeostorePresent() throws Exception
    {
        // Given
        new File( storeDirectory ).mkdirs();
        new File( storeDirectory, "unrelated_file").createNewFile();

        // When
        boolean actual = new StoreRecoverer().recoveryNeededAt( new File( storeDirectory ), 0 );

        // Then
        assertThat("Recovery should not be needed", actual,
                is(false));
    }

    private Config buildProperties() throws IOException
    {
        FileUtils.deleteRecursively( new File( homeDirectory ) );
        new File( homeDirectory + "/conf" ).mkdirs();

        Properties databaseProperties = new Properties();

        String databasePropertiesFileName = homeDirectory + "/conf/neo4j.properties";
        databaseProperties.store( new FileWriter( databasePropertiesFileName ), null );
        
        Config serverProperties = new Config( MapUtil.stringMap( 
                Configurator.DATABASE_LOCATION_PROPERTY_KEY, storeDirectory,
                Configurator.DB_TUNING_PROPERTY_FILE_KEY, databasePropertiesFileName ) );

        return serverProperties;
    }
}
