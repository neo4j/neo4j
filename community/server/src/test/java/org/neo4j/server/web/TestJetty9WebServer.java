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
package org.neo4j.server.web;

import java.io.IOException;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.impl.util.TestLogging;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.SystemOutLogging;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.Mute;

import static org.junit.Assert.assertEquals;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.Mute.muteAll;

public class TestJetty9WebServer
{
    @Test
    public void shouldBeAbleToUsePortZero() throws IOException
    {
        TestLogging logging = new TestLogging();

        Jetty9WebServer webServer = new Jetty9WebServer( logging );

        webServer.setPort( 0 );

        webServer.start();

        webServer.stop();
    }

    @Test
    public void shouldBeAbleToRestart() throws Throwable
    {
        Jetty9WebServer server = new Jetty9WebServer( new SystemOutLogging() );
        try
        {
            server.setAddress( "127.0.0.1" );
            server.setPort( 7878 );

            server.start();
            server.stop();
            server.start();
        }
        finally
        {
            try
            {
                server.stop();
            }
            catch ( Throwable t )
            {

            }
        }
    }

    @Test
    public void shouldBeAbleToSetExecutionLimit() throws Throwable
    {
        @SuppressWarnings("deprecation")
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase( "path", stringMap(),
                GraphDatabaseDependencies.newDependencies() )
        {
        };

        ServerConfigurator config = new ServerConfigurator( db );
        config.configuration().setProperty( Configurator.WEBSERVER_PORT_PROPERTY_KEY, "7476" );
        config.configuration().setProperty( Configurator.WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY, "1000s" );
        WrappingNeoServerBootstrapper testBootstrapper = new WrappingNeoServerBootstrapper( db, config );

        // When
        testBootstrapper.start();
        testBootstrapper.stop();

        // Then it should not have crashed
        // TODO: This is a really poor test, but does not feel worth re-visiting right now since we're removing the
        // guard in subsequent releases.
    }

    @Test
    public void shouldStopCleanlyEvenWhenItHasntBeenStarted()
    {
        new Jetty9WebServer( DevNullLoggingService.DEV_NULL ).stop();
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
        @SuppressWarnings("deprecation")
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase( "path", stringMap(),
                GraphDatabaseDependencies.newDependencies() )
        {
        };

        ServerConfigurator config = new ServerConfigurator( db );
        config.configuration().setProperty( Configurator.WEBSERVER_PORT_PROPERTY_KEY, "7477" );
        WrappingNeoServerBootstrapper testBootstrapper = new WrappingNeoServerBootstrapper( db, config );

        testBootstrapper.start();

        try ( CloseableHttpClient httpClient = HttpClientBuilder.create().build() )
        {
            // Depends on specific resources exposed by the browser module; if this test starts to fail,
            // check whether the structure of the browser module has changed and adjust accordingly.
            assertEquals( 200, httpClient.execute( new HttpGet(
                    "http://localhost:7477/browser/content/help/create.html" ) ).getStatusLine().getStatusCode() );
            assertEquals( 403, httpClient.execute( new HttpGet(
                    "http://localhost:7477/browser/content/help/" ) ).getStatusLine().getStatusCode() );
        }

        testBootstrapper.stop();
    }

    @Rule
    public Mute mute = muteAll();

    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();
}
