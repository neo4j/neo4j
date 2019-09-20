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
package org.neo4j.server.modules;

import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.rest.discovery.DiscoveryService;
import org.neo4j.server.rest.web.AccessiblePathFilter;
import org.neo4j.server.rest.web.CorsFilter;
import org.neo4j.server.web.WebServer;

import static java.util.Collections.singletonList;
import static org.neo4j.server.configuration.ServerSettings.http_access_control_allow_origin;
import static org.neo4j.server.web.Injectable.injectable;

/**
 * Mounts the DBMS REST API.
 */
public class DBMSModule implements ServerModule
{
    private static final String ROOT_PATH = "/";

    private final WebServer webServer;
    private final Config config;
    private final Supplier<DiscoverableURIs> discoverableURIs;
    private LogProvider logProvider;

    public DBMSModule( WebServer webServer, Config config, Supplier<DiscoverableURIs> discoverableURIs, LogProvider logProvider )
    {
        this.webServer = webServer;
        this.config = config;
        this.discoverableURIs = discoverableURIs;
        this.logProvider = logProvider;
    }

    @Override
    public void start()
    {
        webServer.addJAXRSClasses(
                singletonList( DiscoveryService.class ), ROOT_PATH,
                singletonList( injectable( DiscoverableURIs.class, discoverableURIs.get() ) ) );

        webServer.addJAXRSClasses( jaxRsClasses(), ROOT_PATH, null );

        // add filters:
        webServer.addFilter( new CorsFilter( logProvider, config.get( http_access_control_allow_origin ) ), "/*" );
        webServer.addFilter( new AccessiblePathFilter( logProvider, config.get( ServerSettings.http_paths_blacklist ) ), "/*" );
    }

    @Override
    public void stop()
    {
        webServer.removeJAXRSClasses( jaxRsClasses(), ROOT_PATH );
    }

    private List<Class<?>> jaxRsClasses()
    {
        if ( config.get( GraphDatabaseSettings.auth_enabled ) )
        {
            return singletonList( JacksonJsonProvider.class );
        }
        return Collections.emptyList();
    }
}
