/**
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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.junit.Test;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.recovery.StoreRecoverer;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.MapBasedConfiguration;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class TestPerformRecoveryIfNecessary {

	public static final String HOME_DIRECTORY = "target/" + TestPerformRecoveryIfNecessary.class.getSimpleName();
    public static final String STORE_DIRECTORY = HOME_DIRECTORY + "/data/graph.db";

    private static final String LINEBREAK = System.getProperty("line.separator");

	@Test
	public void shouldNotDoAnythingIfNoDBPresent() throws Exception
	{
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		Configuration config = buildProperties();
		PerformRecoveryIfNecessary task = new PerformRecoveryIfNecessary(config, new HashMap<String,String>(), new PrintStream(outputStream), DevNullLoggingService.DEV_NULL);

		assertThat("Recovery task runs successfully.", task.run(), is(true));
		assertThat("No database should have been created.", new File(STORE_DIRECTORY).exists(), is(false));
		assertThat("Recovery task should not print anything.", outputStream.toString(), is(""));
	}

	@Test
	public void doesNotPrintAnythingIfDatabaseWasCorrectlyShutdown() throws Exception
	{
		// Given
		Configuration config = buildProperties();
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIRECTORY).shutdown();

		PerformRecoveryIfNecessary task = new PerformRecoveryIfNecessary(config, new HashMap<String,String>(), new PrintStream(outputStream), DevNullLoggingService.DEV_NULL);

		assertThat("Recovery task should run successfully.", task.run(), is(true));
		assertThat("Database should exist.", new File(STORE_DIRECTORY).exists(), is(true));
		assertThat("Recovery should not print anything.", outputStream.toString(), is(""));
	}

	@Test
	public void shouldPerformRecoveryIfNecessary() throws Exception
	{
		// Given
		StoreRecoverer recoverer = new StoreRecoverer();
		Configuration config = buildProperties();
		ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
		new GraphDatabaseFactory().newEmbeddedDatabase(STORE_DIRECTORY).shutdown();
		// Make this look incorrectly shut down
		new File(STORE_DIRECTORY, "nioneo_logical.log.active").delete();

		assertThat("Store should not be recovered", recoverer.recoveryNeededAt(new File(STORE_DIRECTORY), new HashMap<String,String>()),
				is(true));

		// Run recovery
		PerformRecoveryIfNecessary task = new PerformRecoveryIfNecessary(config, new HashMap<String,String>(), new PrintStream(outputStream), DevNullLoggingService.DEV_NULL);
		assertThat("Recovery task should run successfully.", task.run(), is(true));
		assertThat("Database should exist.", new File(STORE_DIRECTORY).exists(), is(true));
		assertThat("Recovery should print status message.", outputStream.toString(), is("Detected incorrectly shut down database, performing recovery.." + LINEBREAK));
		assertThat("Store should be recovered", recoverer.recoveryNeededAt(new File(STORE_DIRECTORY), new HashMap<String,String>()),
				is(false));
	}

    private Configuration buildProperties() throws IOException
    {
        FileUtils.deleteRecursively( new File( HOME_DIRECTORY ) );
        new File( HOME_DIRECTORY + "/conf" ).mkdirs();

        Properties databaseProperties = new Properties();

        String databasePropertiesFileName = HOME_DIRECTORY + "/conf/neo4j.properties";
        databaseProperties.store( new FileWriter( databasePropertiesFileName ), null );

        Configuration serverProperties = new MapBasedConfiguration();
        serverProperties.setProperty( Configurator.DATABASE_LOCATION_PROPERTY_KEY, STORE_DIRECTORY );
        serverProperties.setProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY, databasePropertiesFileName );

        return serverProperties;
    }

}
