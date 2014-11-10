/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.server.web.WebServer;
import org.neo4j.server.webadmin.rest.JmxService;
import org.neo4j.server.webadmin.rest.MonitorService;
import org.neo4j.server.webadmin.rest.RootService;
import org.neo4j.server.webadmin.rest.VersionAndEditionService;
import org.neo4j.server.webadmin.rest.console.ConsoleService;

import static org.neo4j.server.JAXRSHelper.listFrom;

public class ManagementApiModule implements ServerModule
{
	private final Config config;
	private final WebServer webServer;
    private final ConsoleLogger log;

    public ManagementApiModule(WebServer webServer, Config config, Logging logging)
    {
    	this.webServer = webServer;
    	this.config = config;
    	this.log = logging.getConsoleLog( getClass() );
    }

    @Override
	public void start()
    {
        String serverMountPoint = managementApiUri().toString();
        webServer.addJAXRSClasses( getClassNames(), serverMountPoint, null );
        log.log( "Mounted management API at [%s]", serverMountPoint );
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
    	webServer.removeJAXRSClasses( getClassNames(),
                managementApiUri(  ).toString() );
    }

    private URI managementApiUri( )
    {
        return config.get( ServerInternalSettings.management_api_path );
    }
}
