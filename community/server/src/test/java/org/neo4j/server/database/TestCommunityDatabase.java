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
package org.neo4j.server.database;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.Settings;
import org.neo4j.kernel.StoreLockException;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;
import org.neo4j.shell.ShellSettings;
import org.neo4j.test.Mute;
import org.neo4j.test.BufferingLogging;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.server.ServerTestUtils.createTempDir;
import static org.neo4j.test.Mute.muteAll;

public class TestCommunityDatabase
{
    @Rule
    public Mute mute = muteAll();
    private File databaseDirectory;
    private Database theDatabase;
    private boolean deletionFailureOk;
    private Logging logging;

    @Before
    public void setup() throws Exception
    {
        databaseDirectory = createTempDir();
        logging = new BufferingLogging();
        theDatabase = new CommunityDatabase( configuratorWithServerProperties( stringMap(
                Configurator.DATABASE_LOCATION_PROPERTY_KEY, databaseDirectory.getAbsolutePath() ) ), logging );
    }

    private static Configurator configuratorWithServerProperties( final Map<String, String> serverProperties )
    {
        return new Configurator.Adapter()
        {
            @Override
            public Configuration configuration()
            {
                return new MapConfiguration( serverProperties );
            }
        };
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
        theDatabase.start();

        assertThat( logging.toString(), containsString( "Successfully started database" ) );
    }

    @Test
    public void shouldShutdownCleanly() throws Throwable
    {
        theDatabase.start();
        theDatabase.stop();

        assertThat( logging.toString(), containsString( "Successfully stopped database" ) );
    }

    @Test
    public void shouldComplainIfDatabaseLocationIsAlreadyInUse() throws Throwable
    {
        deletionFailureOk = true;
        theDatabase.start();

        CommunityDatabase db = new CommunityDatabase( configuratorWithServerProperties( stringMap(
                Configurator.DATABASE_LOCATION_PROPERTY_KEY, databaseDirectory.getAbsolutePath() ) ), logging );

        try
        {
            db.start();
        }
        catch ( RuntimeException e )
        {
            // Wrapped in a lifecycle exception, needs to be dug out
            assertThat( e.getCause().getCause(), instanceOf( StoreLockException.class ) );
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
        final int customPort = findFreeShellPortToUse( 8881 );
        final File tempDir = createTempDir();

        Database otherDb = new CommunityDatabase( new Configurator.Adapter()
        {
            @Override
            public Configuration configuration()
            {
                return new MapConfiguration( stringMap(
                        Configurator.DATABASE_LOCATION_PROPERTY_KEY, tempDir.getAbsolutePath() ) );
            }

            @Override
            public Map<String, String> getDatabaseTuningProperties()
            {
                return stringMap(
                      ShellSettings.remote_shell_enabled.name(), Settings.TRUE,
                      ShellSettings.remote_shell_port.name(), "" + customPort );
            }
        }, logging );
        otherDb.start();

        // Try to connect with a shell client to that custom port.
        // Throws exception if unable to connect
        ShellLobby.newClient( customPort )
                .shutdown();

        otherDb.stop();
//        FileUtils.forceDelete( tempDir );
    }

    @Test
    @SuppressWarnings( "deprecation" )
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
                ShellLobby.newClient( startingPort++ ).shutdown();
            }
            catch ( ShellException e )
            {   // Good
                return startingPort;
            }
        }
    }
}
