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
package org.neo4j.server.web;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.DefaultGraphDatabaseDependencies;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.WrappingNeoServer;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.neo4j.test.Mute;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.Mute.muteAll;

@Path("/")
public class TestJetty9WebServer {

	@GET
	public Response index()
	{
		return Response.status( Status.NO_CONTENT )
                .build();
	}

	@Test
	public void shouldBeAbleToRestart() throws Throwable
	{
		// TODO: This is needed because WebServer has a cyclic
		// dependency to NeoServer, which should be removed.
		// Once that is done, we should instantiate WebServer
		// here directly.
        AbstractGraphDatabase db = mock(AbstractGraphDatabase.class);
        when( db.getDependencyResolver() ).thenReturn( noLoggingDependencyResolver() );
		WrappingNeoServer neoServer = new WrappingNeoServer(db);
		WebServer server = neoServer.getWebServer();

		try
		{
			server.setAddress("127.0.0.1");
			server.setPort(7878);

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
            catch( Throwable t )
			{

			}
		}
	}

    private DependencyResolver noLoggingDependencyResolver()
    {
        return new DependencyResolver.Adapter()
        {
            @Override
            public <T> T resolveDependency( Class<T> type, SelectionStrategy selector )
                    throws IllegalArgumentException
            {
                if ( Logging.class.isAssignableFrom( type ) )
                {
                    return type.cast( DevNullLoggingService.DEV_NULL );
                }
                return null;
            }
        };
    }

    @Test
    public void shouldBeAbleToSetExecutionLimit() throws Throwable
    {
        @SuppressWarnings("deprecation")
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase( "path", stringMap(),
                new DefaultGraphDatabaseDependencies())
        {
        };

        ServerConfigurator config = new ServerConfigurator( db );
        config.configuration().setProperty( Configurator.WEBSERVER_PORT_PROPERTY_KEY, 7476 );
        config.configuration().setProperty( Configurator.WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY, 1000 );
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

    @Rule
    public Mute mute = muteAll();
}
