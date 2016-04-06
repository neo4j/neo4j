/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.web;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;
import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class TestJetty9WebServer extends ExclusiveServerTestBase
{
    @Rule
    public SuppressOutput suppressOutput = suppressAll();
    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    private Jetty9WebServer webServer;
    private CommunityNeoServer server;

    @Test
    public void shouldBeAbleToUsePortZero() throws Exception
    {
        // Given
        webServer = new Jetty9WebServer( NullLogProvider.getInstance(), Config.empty() );

        webServer.setAddress( new HostnamePort( "localhost", 0 ) );

        // When
        webServer.start();

        // Then no exception
    }

    @Test
    public void shouldBeAbleToRestart() throws Throwable
    {
        // given
        webServer = new Jetty9WebServer( NullLogProvider.getInstance(), Config.empty() );
        webServer.setAddress( new HostnamePort( "127.0.0.1", 7878 ) );

        // when
        webServer.start();
        webServer.stop();
        webServer.start();

        // then no exception
    }

    @Test
    public void shouldStopCleanlyEvenWhenItHasntBeenStarted()
    {
        new Jetty9WebServer( NullLogProvider.getInstance(), null ).stop();
    }

    /*
     * The default jetty behaviour serves an index page for static resources. The 'directories' exposed through this
     * behaviour are not file system directories, but only a list of resources present on the classpath, so there is no
     * security vulnerability. However, it might seem like a vulnerability to somebody without the context of how the
     * whole stack works, so to avoid confusion we disable the jetty behaviour.
     */
    @Test
    public void shouldDisallowDirectoryListings() throws Exception
    {
        // Given
        server = CommunityServerBuilder.server().build();
        server.start();

        // When
        HTTP.Response okResource = HTTP.GET( server.baseUri().resolve( "/browser/index.html" ).toString() );
        HTTP.Response illegalResource = HTTP.GET( server.baseUri().resolve( "/browser/styles/" ).toString() );

        // Then
        // Depends on specific resources exposed by the browser module; if this test starts to fail,
        // check whether the structure of the browser module has changed and adjust accordingly.
        assertEquals( 200, okResource.status() );
        assertEquals( 403, illegalResource.status() );
    }

    @After
    public void cleanup()
    {
        if ( webServer != null )
        {
            webServer.stop();
        }

        if ( server != null )
        {
            server.stop();
        }
    }

}
