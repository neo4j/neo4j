/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.File;

import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.server.helpers.CommunityServerBuilder;
import org.neo4j.test.ImpermanentDatabaseRule;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.server.ExclusiveServerTestBase;
import org.neo4j.test.server.HTTP;

import static org.junit.Assert.assertEquals;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.SuppressOutput.suppressAll;

public class TestJetty9WebServer extends ExclusiveServerTestBase
{
    private Jetty9WebServer webServer;
    private CommunityNeoServer server;

    @Test
    public void shouldBeAbleToUsePortZero() throws Exception
    {
        // Given
        webServer = new Jetty9WebServer( NullLogProvider.getInstance(), new Config() );

        webServer.setPort( 0 );

        // When
        webServer.start();

        // Then no exception
    }

    @Test
    public void shouldBeAbleToRestart() throws Throwable
    {
        // given
        webServer = new Jetty9WebServer( NullLogProvider.getInstance(), new Config() );
        webServer.setAddress( "127.0.0.1" );
        webServer.setPort( 7878 );

        // when
        webServer.start();
        webServer.stop();
        webServer.start();

        // then no exception
    }

    @Test
    public void shouldBeAbleToSetExecutionLimit() throws Throwable
    {
        @SuppressWarnings("deprecation")
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase( new File( "path" ), stringMap(),
                GraphDatabaseDependencies.newDependencies() );

        ServerConfigurator config = new ServerConfigurator( db );
        config.configuration().setProperty( Configurator.WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY, "1000s" );
        WrappingNeoServerBootstrapper testBootstrapper = new WrappingNeoServerBootstrapper( db, config );

        // When
        testBootstrapper.start();
        testBootstrapper.stop();

        // Then it should not have crashed
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
        HTTP.Response okResource = HTTP.GET( server.baseUri().resolve( "/browser/content/help/create.html" ).toString() );
        HTTP.Response illegalResource = HTTP.GET( server.baseUri().resolve( "/browser/content/help/" ).toString() );

        // Then
        // Depends on specific resources exposed by the browser module; if this test starts to fail,
        // check whether the structure of the browser module has changed and adjust accordingly.
        assertEquals( 200, okResource.status() );
        assertEquals( 403, illegalResource.status() );
    }

    @Rule
    public SuppressOutput suppressOutput = suppressAll();

    @Rule
    public ImpermanentDatabaseRule dbRule = new ImpermanentDatabaseRule();

    @After
    public void cleanup()
    {
        if( webServer != null )
        {
            webServer.stop();
        }

        if( server != null )
        {
            server.stop();
        }
    }

}
