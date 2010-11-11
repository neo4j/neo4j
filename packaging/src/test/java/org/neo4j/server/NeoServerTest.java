/**
 * Copyright (c) 2002-2010 "Neo Technology,"
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

package org.neo4j.server;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckFailedException;

public class NeoServerTest {
    
    private InMemoryAppender appender;
    public NeoServer server;

    @Before
    public void setup() {
        ServerTestUtils.nukeServer();
        appender = new InMemoryAppender(NeoServer.log);
        server = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
    }
    
    @After
    public void tearDown() {
        ServerTestUtils.nukeServer();
    }

    @Test
    public void whenServerIsStartedItshouldStartASingleDatabase() throws Exception {
        assertNotNull(server.database());
    }

    @Test
    public void shouldLogStartup() throws Exception {
        assertThat(appender.toString(), containsString("Started Neo Server on port [" + server.restApiUri().getPort() + "]"));
    }

    @Test(expected = NullPointerException.class)
    public void whenServerIsShutDownTheDatabaseShouldNotBeAvailable() throws IOException {

        
        // Do some work
        server.database().db.beginTx().success();
        server.stop();

        server.database().db.beginTx();
    }

    
    @Test(expected=StartupHealthCheckFailedException.class)
    public void shouldExitWhenFailedStartupHealthCheck() {
        System.clearProperty(NeoServer.NEO_CONFIG_FILE_KEY);
        new NeoServer();
    }
}
