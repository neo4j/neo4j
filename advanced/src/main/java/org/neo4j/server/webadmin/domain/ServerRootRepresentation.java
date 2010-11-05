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

package org.neo4j.server.webadmin.domain;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.remote.RemoteGraphDatabase;
import org.neo4j.rest.domain.DatabaseBlockedException;
import org.neo4j.server.NeoServer;
import org.neo4j.server.webadmin.rest.ConfigService;
import org.neo4j.server.webadmin.rest.ConsoleService;
import org.neo4j.server.webadmin.rest.ExportService;
import org.neo4j.server.webadmin.rest.ImportService;
import org.neo4j.server.webadmin.rest.JmxService;
import org.neo4j.server.webadmin.rest.LifecycleService;
import org.neo4j.server.webadmin.rest.MonitorService;

public class ServerRootRepresentation extends RootRepresentation
{

    public enum Mode
    {
        EMBEDDED
    }

    private Mode mode;

    public ServerRootRepresentation( URI baseUri, Mode mode )
    {
        super( baseUri );
        this.mode = mode;
    }

    public Object serialize()
    {
        Map<String, Object> def = new HashMap<String, Object>();
        Map<String, Object> services = new HashMap<String, Object>();

        GraphDatabaseService currentDb;
        try
        {
            currentDb = NeoServer.INSTANCE.database();
            if ( currentDb instanceof EmbeddedGraphDatabase )
            {

                //services.put( "backup", baseUri + BackupService.ROOT_PATH );
                //services.put( "config", baseUri + ConfigService.ROOT_PATH );
                services.put( "importing", baseUri + ImportService.ROOT_PATH );
                services.put( "exporting", baseUri + ExportService.ROOT_PATH );
                services.put( "console", baseUri + ConsoleService.ROOT_PATH );
                services.put( "jmx", baseUri + JmxService.ROOT_PATH );
                //services.put( "lifecycle", baseUri + LifecycleService.ROOT_PATH );
                services.put( "monitor", baseUri + MonitorService.ROOT_PATH );

            }
            else if ( currentDb instanceof RemoteGraphDatabase )
            {
                // services.put( "backup", baseUri + BackupService.ROOT_PATH );
                services.put( "importing", baseUri + ImportService.ROOT_PATH );
                services.put( "config", baseUri + ConfigService.ROOT_PATH );
                services.put( "exporting", baseUri + ExportService.ROOT_PATH );
                services.put( "console", baseUri + ConsoleService.ROOT_PATH );
                // services.put( "monitor", baseUri + MonitorService.ROOT_PATH
                // );
            }
        }
        catch ( DatabaseBlockedException e )
        {
            services.put( "lifecycle", baseUri + LifecycleService.ROOT_PATH );
        }

        def.put( "services", services );
        return def;
    }
}
