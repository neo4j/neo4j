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

import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.http.cypher.CypherResource;
import org.neo4j.server.http.cypher.format.input.json.JsonMessageBodyReader;
import org.neo4j.server.http.cypher.format.output.json.JsonMessageBodyWriter;
import org.neo4j.server.rest.web.CorsFilter;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.configuration.ServerSettings.http_access_control_allow_origin;

/**
 * Mounts the database REST API.
 */
public class RESTApiModule implements ServerModule
{
    private final Config config;
    private final WebServer webServer;
    private final LogProvider logProvider;

    public RESTApiModule( WebServer webServer, Config config, LogProvider logProvider )
    {
        this.webServer = webServer;
        this.config = config;
        this.logProvider = logProvider;
    }

    @Override
    public void start()
    {
        URI restApiUri = restApiUri( );

        webServer.addFilter( new CorsFilter( logProvider, config.get( http_access_control_allow_origin ) ), "/*" );
        webServer.addJAXRSClasses( getClassNames(), restApiUri.toString(), null );
    }

    private List<Class<?>> getClassNames()
    {
        return Arrays.asList(
                CypherResource.class,
                JsonMessageBodyReader.class,
                JsonMessageBodyWriter.class );
    }

    @Override
    public void stop()
    {
        webServer.removeJAXRSClasses( getClassNames(), restApiUri().toString() );
    }

    private URI restApiUri()
    {
        return config.get( ServerSettings.rest_api_path );
    }

}
