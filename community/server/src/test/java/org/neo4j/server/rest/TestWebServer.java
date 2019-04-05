/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.server.rest;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;

import java.util.HashSet;
import java.util.List;

public class TestWebServer
{
    private final Server server = new Server( 0 );

    public TestWebServer( String path, List<Class<?>> classes, List<Object> instances )
    {
        ResourceConfig resourceConfig = new ResourceConfig()
                .registerClasses( new HashSet<>( classes ) )
                .registerInstances( new HashSet<>( instances ) );

        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath( "/*" );
        server.setHandler( context );

        ServletContainer container = new ServletContainer( resourceConfig );
        context.addServlet( new ServletHolder( container ), path );
    }

    public void start()
    {
        try
        {
            server.start();
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Failed to start the server", e );
        }
    }

    public int getPort()
    {
        return ((ServerConnector) server.getConnectors()[0]).getLocalPort();
    }

    public void stop()
    {
        try
        {
            server.stop();
        }
        catch ( Exception e )
        {
            throw new IllegalStateException( "Failed to stop the server", e );
        }
    }
}
