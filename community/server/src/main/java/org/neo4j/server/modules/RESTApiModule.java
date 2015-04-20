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
package org.neo4j.server.modules;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.guard.GuardingRequestFilter;
import org.neo4j.server.plugins.PluginManager;
import org.neo4j.server.rest.web.BatchOperationService;
import org.neo4j.server.rest.web.CypherService;
import org.neo4j.server.rest.web.DatabaseMetadataService;
import org.neo4j.server.rest.web.ExtensionService;
import org.neo4j.server.rest.web.ResourcesService;
import org.neo4j.server.rest.web.RestfulGraphDatabase;
import org.neo4j.server.rest.web.TransactionalService;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.JAXRSHelper.listFrom;

/**
 * Mounts the database REST API.
 */
public class RESTApiModule implements ServerModule
{
    private PluginManager plugins;
    private final Config config;
    private final WebServer webServer;
    private final Database database;
    private GuardingRequestFilter requestTimeLimitFilter;
    private final ConsoleLogger log;
    private final Logging logging;

    public RESTApiModule( WebServer webServer, Database database, Config config, Logging logging )
    {
        this.webServer = webServer;
        this.config = config;
        this.database = database;
        this.logging = logging;
        this.log = logging.getConsoleLog( getClass() );
    }

    @Override
    public void start()
    {
        try
        {
            URI restApiUri = restApiUri( );

            webServer.addJAXRSClasses( getClassNames(), restApiUri.toString(), null );
            loadPlugins();

            setupRequestTimeLimit();
        }
        catch ( URISyntaxException e )
        {
            log.warn( "Unable to mount REST API", e );
        }
    }

    private List<String> getClassNames()
    {
        return listFrom(
                RestfulGraphDatabase.class.getName(),
                TransactionalService.class.getName(),
                CypherService.class.getName(),
                DatabaseMetadataService.class.getName(),
                ExtensionService.class.getName(),
                ResourcesService.class.getName(),
                BatchOperationService.class.getName() );
    }

    @Override
    public void stop()
    {
        try
        {
            webServer.removeJAXRSClasses( getClassNames(), restApiUri().toString() );

        tearDownRequestTimeLimit();
        unloadPlugins();
        }
        catch ( URISyntaxException e )
        {
          log.warn( "Unable to unmount REST API", e );
        }
    }

    private void tearDownRequestTimeLimit() {
        if(requestTimeLimitFilter != null)
        {
            webServer.removeFilter(requestTimeLimitFilter, "/*");
        }
    }

    private void setupRequestTimeLimit() {
        Long limit = config.get( ServerSettings.webserver_limit_execution_time );
        
        if ( limit != null )
        {
            try
            {
                Guard guard = database.getGraph().getDependencyResolver().resolveDependency( Guard.class );
                this.requestTimeLimitFilter = new GuardingRequestFilter( guard, limit );
                webServer.addFilter(requestTimeLimitFilter , "/*" );
            }
            catch ( IllegalArgumentException e )
            {
                //TODO enable guard and restart EmbeddedGraphdb
                throw new RuntimeException( "Unable to use guard, you have to enable guard in neo4j.properties", e );
            }
        }
    }

    private URI restApiUri() throws URISyntaxException
    {
        return config.get( ServerInternalSettings.rest_api_path );
    }

    private void loadPlugins()
    {
        plugins = new PluginManager( config, logging );
    }

    private void unloadPlugins() {
        // TODO
    }

    public PluginManager getPlugins()
    {
        return plugins;
    }
}
