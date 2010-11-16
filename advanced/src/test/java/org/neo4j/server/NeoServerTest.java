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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.net.ServerSocket;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.server.startup.healthcheck.StartupHealthCheckFailedException;

public class NeoServerTest
{

    private InMemoryAppender appender;
    public NeoServer server;

    @Before
    public void setup() throws ServerStartupException
    {
        ServerTestUtils.nukeServer();
        appender = new InMemoryAppender( NeoServer.log );
        server = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory();
    }

    @After
    public void tearDown()
    {
        ServerTestUtils.nukeServer();
    }
    
    @Test
    public void whenServerIsStartedItshouldStartASingleDatabase() throws Exception
    {
        assertNotNull( server.database() );
    }

    @Test
    public void shouldLogStartup() throws Exception
    {
        assertThat( appender.toString(), containsString( "Started Neo Server on port [" + server.restApiUri().getPort() + "]" ) );
    }

    @Test(expected = NullPointerException.class)
    public void whenServerIsShutDownTheDatabaseShouldNotBeAvailable() throws IOException
    {


        // Do some work
        server.database().graph.beginTx().success();
        server.stop();

        server.database().graph.beginTx();
    }


    @Test(expected = StartupHealthCheckFailedException.class)
    public void shouldExitWhenFailedStartupHealthCheck()
    {
        System.clearProperty( NeoServer.NEO_CONFIG_FILE_KEY );
        new NeoServer();
    }

    @Test
    public void shouldExitWhenDatabaseLocationIsInUse()
    {
        NeoServer firstServer = NeoServer.getServer_FOR_TESTS_ONLY_KITTENS_DIE_WHEN_YOU_USE_THIS();

        ServerStartupException thrownException = null;
        try
        {
            NeoServer.main(null);
        } catch ( ServerStartupException e )
        {
            thrownException = e;
        }

        assertThat( thrownException.getErrorCode(), equalTo( NeoServer.GRAPH_DATABASE_STARTUP_ERROR_CODE ) );

        firstServer.stop();
    }

    @Test
    public void shouldExitWhenPortIsInUse() throws IOException
    {
        NeoServer extraneousServerCreatedByTestSetup = NeoServer.getServer_FOR_TESTS_ONLY_KITTENS_DIE_WHEN_YOU_USE_THIS();
        ServerSocket preBoundSocket = new ServerSocket( 8697 );
        ServerStartupException thrownException = null;
        try
        {
            NeoServer cheatedServer = ServerTestUtils.initializeServerWithRandomTemporaryDatabaseDirectory( Integer.toString( preBoundSocket.getLocalPort() ) );
            cheatedServer.start();
        } catch ( ServerStartupException e )
        {
            thrownException = e;
        }
        assertThat( thrownException.getErrorCode(), is( equalTo( NeoServer.WEB_SERVER_STARTUP_ERROR_CODE ) ) );
        preBoundSocket.close();
        extraneousServerCreatedByTestSetup.stop();

    }
}
