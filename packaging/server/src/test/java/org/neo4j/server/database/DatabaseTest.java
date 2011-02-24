/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.database;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.logging.InMemoryAppender;

import java.io.File;
import java.io.IOException;
import java.net.Socket;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class DatabaseTest {

    private File databaseDirectory;
    private Database theDatabase;

    @Before
    public void setup() throws Exception {
        databaseDirectory = ServerTestUtils.createTempDir();
        theDatabase = new Database( DatabaseMode.STANDALONE, databaseDirectory.getAbsolutePath() );
    }

    @After
    public void shutdownDatabase() throws IOException
    {
        this.theDatabase.shutdown();
        FileUtils.forceDelete( databaseDirectory );
    }

    @Test
    public void shouldLogOnSuccessfulStartup() {
        InMemoryAppender appender = new InMemoryAppender(Database.log);

        theDatabase.startup();

        assertThat(appender.toString(), containsString("Successfully started database"));
    }


    @Test
    public void shouldShutdownCleanly() {
        InMemoryAppender appender = new InMemoryAppender(Database.log);

        theDatabase.startup();
        theDatabase.shutdown();

        assertThat(appender.toString(), containsString("Successfully shutdown database"));
    }

    @Test(expected = TransactionFailureException.class)
    public void shouldComplainIfDatabaseLocationIsAlreadyInUse() {
        new Database( DatabaseMode.STANDALONE, theDatabase.getLocation() );
    }

    @Test
    @Ignore
    public void shouldEnableRemoteShellByDefault() throws IOException
    {
        int wellKnownSocket = 1337;
        Socket rmiSocketToServer = new Socket("localhost", wellKnownSocket );
        assertTrue(rmiSocketToServer.isBound());
        rmiSocketToServer.close();
    }
}
