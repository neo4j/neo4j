/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static org.neo4j.server.JAXRSHelper.listFrom;
import static org.neo4j.server.configuration.Configurator.WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.List;

import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.guard.Guard;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.guard.GuardingRequestFilter;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.plugins.Injectable;
import org.neo4j.server.plugins.PluginManager;
import org.neo4j.server.web.WebServer;

public class RESTApiModule implements ServerModule
{
    private static final Logger log = Logger.getLogger( RESTApiModule.class );
    private PluginManager plugins;
	private final Configuration config;
	private final WebServer webServer;
	private final Database database;
	private GuardingRequestFilter requestTimeLimitFilter;

    public RESTApiModule(WebServer webServer, Database database, Configuration config)
    {
    	this.webServer = webServer;
    	this.config = config;
    	this.database = database;
    }

    @Override
	public void start( StringLogger logger )
    {
        try
        {
            URI restApiUri = restApiUri( );

            webServer.addJAXRSPackages( getPackageNames(), restApiUri.toString(), null );
            loadPlugins( logger );
            
            setupRequestTimeLimit();

            log.info( "Mounted REST API at [%s]", restApiUri.toString() );
            if ( logger != null ) logger.logMessage( "Mounted REST API at: " + restApiUri.toString() );
        }
        catch ( URISyntaxException e )
        {
            log.warn( e );
        }
    }

    private List<String> getPackageNames()
    {
        return listFrom( new String[] { Configurator.REST_API_PACKAGE } );
    }

    @Override
	public void stop()
    {
        try
        {
			webServer.removeJAXRSPackages( getPackageNames(), restApiUri().toString() );

			tearDownRequestTimeLimit();
			unloadPlugins();
	    }
	    catch ( URISyntaxException e )
	    {
	        log.warn( e );
	    }
    }

	private void tearDownRequestTimeLimit() {
		if(requestTimeLimitFilter != null)
		{
			webServer.removeFilter(requestTimeLimitFilter, "/*");
		}
	}

	private void setupRequestTimeLimit() {
    	Integer limit = config.getInteger( WEBSERVER_LIMIT_EXECUTION_TIME_PROPERTY_KEY, null );
        if ( limit != null )
        {
        	Guard guard = database.getGraph().getGuard();
        	if ( guard == null )
            {
                //TODO enable guard and restart EmbeddedGraphdb
                throw new RuntimeException( "Unable to use guard, you have to enable guard in neo4j.properties" );
            }
        	
        	this.requestTimeLimitFilter = new GuardingRequestFilter( guard, limit );
            webServer.addFilter(requestTimeLimitFilter , "/*" );
        }
	}

    private URI restApiUri() throws URISyntaxException
    {
        return new URI( config.getString( Configurator.REST_API_PATH_PROPERTY_KEY, Configurator.DEFAULT_DATA_API_PATH ) );
    }

    private void loadPlugins( StringLogger logger )
    {
        plugins = new PluginManager( config, logger );
    }

    private void unloadPlugins() {
		// TODO
	}

    public PluginManager getPlugins()
    {
        return plugins;
    }
}
