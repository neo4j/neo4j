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

package org.neo4j.webadmin;

import java.io.File;

import org.neo4j.rest.WebServer;
import org.neo4j.rest.web.AllowAjaxFilter;
import org.neo4j.webadmin.rest.ContentDispositionFilter;

import com.sun.grizzly.http.embed.GrizzlyWebServer;
import com.sun.grizzly.http.servlet.ServletAdapter;
import com.sun.grizzly.tcp.http11.GrizzlyAdapter;
import com.sun.grizzly.tcp.http11.GrizzlyRequest;
import com.sun.grizzly.tcp.http11.GrizzlyResponse;
import com.sun.jersey.api.core.ResourceConfig;
import com.sun.jersey.spi.container.servlet.ServletContainer;

/**
 * Launcher for the Grizzly server that handles the admin interface. This code
 * based on {@link WebServer} in the neo4j REST interface.
 * 
 * @author Jacob Hansson <jacob@voltvoodoo.com>
 * 
 */
public enum AdminServer
{
    INSTANCE;

    public static final int DEFAULT_PORT = 9988;
    public static final String DEFAULT_STATIC_PATH = "public";

    private GrizzlyWebServer server;
    private int port = DEFAULT_PORT;
    private String staticPath;

    public void startServer()
    {
        startServer( DEFAULT_PORT, DEFAULT_STATIC_PATH );
    }

    public void startServer( int port, String staticPath )
    {
        try
        {
            this.port = port;
            this.staticPath = ( new File( staticPath ) ).getAbsolutePath();

            // Instantiate the server
            server = new GrizzlyWebServer( port, this.staticPath );

            // Create REST-adapter
            ServletAdapter jerseyAdapter = new ServletAdapter();
            jerseyAdapter.addInitParameter(
                    "com.sun.jersey.config.property.packages",
                    "org.neo4j.webadmin.rest" );
            jerseyAdapter.setContextPath( "/manage" );
            jerseyAdapter.setServletInstance( new ServletContainer() );

            // Configure response filters
            jerseyAdapter.addInitParameter(
                    ResourceConfig.PROPERTY_CONTAINER_RESPONSE_FILTERS,
                    AllowAjaxFilter.class.getName() );

            // Add adapters
            server.addGrizzlyAdapter( jerseyAdapter, new String[] { "/manage" } );

            /*
             * This is an awful hack. If any adapters are added to grizzly, 
             * it stops serving static files.
             * 
             * All GrizzlyAdapters are 
             * static file serving adapters if you flip a flag. So to make 
             * grizzly serve static files, we create an empty grizzly adapter, 
             * tell it to serve static files and add it here. 
             * 
             * §%&"#"#"%¤#&!&"/#.
             */
            GrizzlyAdapter staticAdapter = new GrizzlyAdapter( this.staticPath )
            {
                public void service( GrizzlyRequest req, GrizzlyResponse res )
                {

                }
            };
            staticAdapter.setHandleStaticResources( true );
            server.addGrizzlyAdapter( staticAdapter, new String[] { "" } );

            server.addAsyncFilter( new ContentDispositionFilter() );

            // Start server
            server.start();

        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }

    /**
     * @return Path to the folder from which we are serving static content.
     */
    public String getStaticPath()
    {
        return this.staticPath;
    }

    public String getBaseUri()
    {
        return getLocalhostBaseUri( port );
    }

    public void stopServer()
    {
        server.stop();
    }

    public int getPort()
    {
        return port;
    }

    public static String getLocalhostBaseUri()
    {
        return getLocalhostBaseUri( DEFAULT_PORT );
    }

    public static String getLocalhostBaseUri( int port )
    {
        return "http://localhost:" + port + "/";
    }

}
