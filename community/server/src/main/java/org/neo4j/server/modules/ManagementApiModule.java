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
import java.util.List;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.management.JmxService;
import org.neo4j.server.rest.management.RootService;
import org.neo4j.server.rest.management.VersionAndEditionService;
import org.neo4j.server.web.WebServer;

import static java.util.Arrays.asList;

public class ManagementApiModule implements ServerModule
{
    private final Config config;
    private final WebServer webServer;

    public ManagementApiModule( WebServer webServer, Config config )
    {
        this.webServer = webServer;
        this.config = config;
    }

    @Override
    public void start()
    {
        String serverMountPoint = managementApiUri().toString();
        webServer.addJAXRSClasses( getClassNames(), serverMountPoint, null );
    }

    private List<String> getClassNames()
    {
        return asList(
                JmxService.class.getName(),
                RootService.class.getName(),
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
        return config.get( ServerSettings.management_api_path );
    }
}
