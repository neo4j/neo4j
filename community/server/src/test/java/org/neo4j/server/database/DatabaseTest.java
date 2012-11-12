/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import static org.junit.Assert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.Config.ENABLE_REMOTE_SHELL;
import static org.neo4j.server.ServerTestUtils.EMBEDDED_GRAPH_DATABASE_FACTORY;
import static org.neo4j.server.ServerTestUtils.createTempDir;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.shell.ShellException;
import org.neo4j.shell.ShellLobby;

public class DatabaseTest
{
    private File databaseDirectory;
    private Database theDatabase;
    private boolean deletionFailureOk;

    @Before
    public void setup() throws Exception
    {
        databaseDirectory = createTempDir();
        theDatabase = new Database( EMBEDDED_GRAPH_DATABASE_FACTORY, databaseDirectory.getAbsolutePath() );
    }

    @After
    public void shutdownDatabase() throws IOException
    {
        this.theDatabase.shutdown();

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
    public void shouldLogOnSuccessfulStartup()
    {
        InMemoryAppender appender = new InMemoryAppender( Database.log );

        theDatabase.startup();

        assertThat( appender.toString(), containsString( "Successfully started database" ) );
    }

    @Test
    public void shouldShutdownCleanly()
    {
        InMemoryAppender appender = new InMemoryAppender( Database.log );

        theDatabase.startup();
        theDatabase.shutdown();

        assertThat( appender.toString(), containsString( "Successfully shutdown database" ) );
    }

    @Test( expected = TransactionFailureException.class )
    public void shouldComplainIfDatabaseLocationIsAlreadyInUse()
    {
        deletionFailureOk = true;
        new Database( EMBEDDED_GRAPH_DATABASE_FACTORY, theDatabase.getLocation() );
    }

    @Test
    public void connectWithShellOnDefaultPortWhenNoShellConfigSupplied() throws Exception
    {
        ShellLobby.newClient()
                .shutdown();
    }

    @Test
    public void shouldBeAbleToOverrideShellConfig() throws Exception
    {
        int customPort = findFreeShellPortToUse( 8881 );
        File tempDir = createTempDir();
        Database otherDb = new Database( EMBEDDED_GRAPH_DATABASE_FACTORY, tempDir.getAbsolutePath(), stringMap(
                ENABLE_REMOTE_SHELL, "port=" + customPort ) );
        otherDb.startup();

        // Try to connect with a shell client to that custom port.
        // Throws exception if unable to connect
        ShellLobby.newClient( customPort )
                .shutdown();

        otherDb.shutdown();
        FileUtils.forceDelete( tempDir );
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
