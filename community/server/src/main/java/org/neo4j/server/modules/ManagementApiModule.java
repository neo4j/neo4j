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
package org.neo4j.server.modules;

import java.net.URI;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.commons.configuration.Configuration;

import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.web.WebServer;
import org.neo4j.server.webadmin.rest.JmxService;
import org.neo4j.server.webadmin.rest.MonitorService;
import org.neo4j.server.webadmin.rest.RootService;
import org.neo4j.server.webadmin.rest.VersionAndEditionService;
import org.neo4j.server.webadmin.rest.console.ConsoleService;

import static org.neo4j.server.JAXRSHelper.listFrom;

public class ManagementApiModule implements ServerModule
{
    private final Logger log = Logger.getLogger( ManagementApiModule.class );

    private final Configuration config;
    private final WebServer webServer;

    public ManagementApiModule( WebServer webServer, Configuration config )
    {
        this.webServer = webServer;
        this.config = config;
    }

    @Override
    public void start( StringLogger logger )
    {
        try
        {
            String serverMountPoint = managementApiUri().toString();
            webServer.addJAXRSClasses( getClassNames(), serverMountPoint, null );
            log.info( "Mounted management API at [%s]", serverMountPoint );
            if ( logger != null )
            {
                logger.logMessage( "Mounted management API at: " + serverMountPoint );
            }
        }
        catch ( UnknownHostException e )
        {
            log.warn( e );
        }
    }

    private List<String> getClassNames()
    {
        return listFrom(
                JmxService.class.getName(),
                MonitorService.class.getName(),
                RootService.class.getName(),
                ConsoleService.class.getName(),
                VersionAndEditionService.class.getName() );
    }

    @Override
    public void stop()
    {
        try
        {
            webServer.removeJAXRSClasses( getClassNames(),
                    managementApiUri().toString() );
        }
        catch ( UnknownHostException e )
        {
            log.warn( e );
        }
    }

    private URI managementApiUri() throws UnknownHostException
    {
        return URI.create( config.getString( Configurator.MANAGEMENT_PATH_PROPERTY_KEY,
                Configurator.DEFAULT_MANAGEMENT_API_PATH ) );
    }
}
