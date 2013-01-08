/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.ServerTestUtils.createTempDir;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.factory.GraphDatabaseSetting;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.ShellSettings;

public class TestCommunityDatabase
{
    private File databaseDirectory;
    private Database theDatabase;
    private boolean deletionFailureOk;

    @Before
    public void setup() throws Exception
    {
        databaseDirectory = createTempDir();
        Configuration conf = new MapConfiguration(new HashMap<String,String>());
        conf.addProperty(Configurator.DATABASE_LOCATION_PROPERTY_KEY, databaseDirectory.getAbsolutePath());
        theDatabase = new CommunityDatabase( conf );
    }

    @After
    public void shutdownDatabase() throws Throwable
    {
        this.theDatabase.stop();

        try
        {
            FileUtils.forceDelete( databaseDirectory );
        }
        catch ( IOException e )
        {
            // TODO Removed this when EmbeddedGraphDatabase startup failures
            // closes its
            // files properly.
            if ( !deletionFailureOk )
            {
                throw e;
            }
        }
    }

    @Test
    public void shouldLogOnSuccessfulStartup() throws Throwable
    {
        InMemoryAppender appender = new InMemoryAppender( Database.log );

        theDatabase.start();

        assertThat( appender.toString(), containsString( "Successfully started database" ) );
    }

    @Test
    public void shouldShutdownCleanly() throws Throwable
    {
        InMemoryAppender appender = new InMemoryAppender( Database.log );

        theDatabase.start();
        theDatabase.stop();

        assertThat( appender.toString(), containsString( "Successfully stopped database" ) );
    }

    @Test
    public void shouldComplainIfDatabaseLocationIsAlreadyInUse() throws Throwable
    {
        deletionFailureOk = true;
        theDatabase.start();
        
        Configuration conf = new MapConfiguration(new HashMap<String,String>());
        conf.addProperty(Configurator.DATABASE_LOCATION_PROPERTY_KEY, databaseDirectory.getAbsolutePath());
        CommunityDatabase db = new CommunityDatabase( conf );

        try
        {
            db.start();
        }
        catch ( RuntimeException e )
        {
            // Wrapped in a lifecycle exception, needs to be dug out
            assertTrue( IllegalStateException.class.isAssignableFrom( e.getCause().getCause().getCause().getClass() ) );
        }
    }

    @Test
    public void connectWithShellOnDefaultPortWhenNoShellConfigSupplied() throws Throwable
    {
    	theDatabase.start();
        ShellLobby.newClient()
                .shutdown();
    }

    @Test
    public void shouldBeAbleToOverrideShellConfig()  throws Throwable
    {
        int customPort = findFreeShellPortToUse( 8881 );
        File tempDir = createTempDir();
        File tuningProperties = new File(tempDir, "neo4j.properties");
        tuningProperties.createNewFile();
        
        ServerTestUtils.writePropertiesToFile(stringMap(
            ShellSettings.remote_shell_enabled.name(), GraphDatabaseSetting.TRUE,
            ShellSettings.remote_shell_port.name(), ""+customPort ), 
            tuningProperties);
        
        Configuration conf = new MapConfiguration(new HashMap<String,String>());
        conf.addProperty(Configurator.DATABASE_LOCATION_PROPERTY_KEY, tempDir.getAbsolutePath());
        conf.addProperty(Configurator.DB_TUNING_PROPERTY_FILE_KEY, tuningProperties.getAbsolutePath());
        
        Database otherDb = new CommunityDatabase( conf );
        otherDb.start();

        // Try to connect with a shell client to that custom port.
        // Throws exception if unable to connect
        ShellLobby.newClient( customPort )
                .shutdown();

        otherDb.stop();
        FileUtils.forceDelete( tempDir );
    }

    @Test
    public void shouldBeAbleToGetLocation() throws Throwable
    {

        theDatabase.start();
        assertThat( theDatabase.getLocation(), is( theDatabase.getGraph().getStoreDir() ) );

    }

    private int findFreeShellPortToUse( int startingPort )
    {
        // Make sure there's no other random stuff on that port
        while ( true )
        {
            try
            {
                ShellLobby.newClient( startingPort )
                        .shutdown();
                startingPort++;
            }
            catch ( ShellException e )
            { // Good
                return startingPort;
            }
        }
    }
}
