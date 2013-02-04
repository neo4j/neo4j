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
package org.neo4j.server.web;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.Node;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.impl.util.TestStringLogger;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.WrappingNeoServer;
import org.neo4j.server.WrappingNeoServerBootstrapper;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.configuration.ServerConfigurator;
import org.neo4j.server.configuration.ThirdPartyJaxRsPackage;
import org.neo4j.server.guard.GuardingRequestFilter;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.server.logging.InMemoryAppender;
import org.neo4j.test.ImpermanentGraphDatabase;

@Path("/")
public class TestJetty6WebServer {

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
		WrappingNeoServer neoServer = new WrappingNeoServer(db);
		WebServer server = neoServer.getWebServer();
		
		try 
		{
			server.setAddress("127.0.0.1");
			server.setPort(7878);
			
			server.addJAXRSPackages(Arrays.asList(new String[]{"org.neo4j.server.web"}), "/", null );
			
			server.start();
			server.stop();
			server.start();
		} finally 
		{
			try 
			{
				server.stop();
			} catch(Throwable t)
			{	
				
			}
		}
		
	}

    @Test
    public void shouldBeAbleToSetExecutionLimit() throws Throwable
    {
        InMemoryAppender appender = new InMemoryAppender(AbstractNeoServer.log);
        final Guard dummyGuard = new Guard(StringLogger.SYSTEM);
        ImpermanentGraphDatabase db = new ImpermanentGraphDatabase()
        {
            @Override
            public Node getNodeById( long id )
            {
                try
                {
                    Thread.sleep( 100 );
                } catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
                return super.getNodeById( id );
            }

            @Override
            public Guard getGuard() {
                return dummyGuard;
            }
        };

        ServerConfigurator config = new ServerConfigurator( db );
        config.configuration().setProperty( Configurator.WEBSERVER_PORT_PROPERTY_KEY, 7476 );
        config.configuration().setProperty( Configurator.WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY, 1000 );
        WrappingNeoServerBootstrapper testBootstrapper = new WrappingNeoServerBootstrapper( db, config );
        testBootstrapper.start();
        assertThat( appender.toString(), containsString( "Server started on" ) );
        testBootstrapper.stop();
    }
	
}
