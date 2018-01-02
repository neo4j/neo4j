/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.ManagementApiModule;
import org.neo4j.server.modules.Neo4jBrowserModule;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.SecurityRulesModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.modules.WebAdminModule;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.management.JmxService;
import org.neo4j.server.rest.management.MonitorService;
import org.neo4j.server.rest.management.console.ConsoleService;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.server.web.ServerInternalSettings;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

/**
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 * Please use Neo4j Server and plugins or un-managed extensions for bespoke solutions.
 */
@Deprecated
public class CommunityNeoServer extends AbstractNeoServer
{
    public static final GraphFactory COMMUNITY_FACTORY = new GraphFactory()
    {
        @Override
        public GraphDatabaseAPI newGraphDatabase( Config config, GraphDatabaseFacadeFactory.Dependencies dependencies )
        {
            File storeDir = config.get( ServerInternalSettings.legacy_db_location );
            return new CommunityFacadeFactory().newFacade( storeDir, config.getParams(), dependencies );
        }
    };

    public CommunityNeoServer( Config config, GraphDatabaseFacadeFactory.Dependencies dependencies,
            LogProvider logProvider )
    {
        this( config, lifecycleManagingDatabase( COMMUNITY_FACTORY ), dependencies, logProvider );
    }

    public CommunityNeoServer( Config config, Database.Factory dbFactory, GraphDatabaseFacadeFactory.Dependencies
            dependencies, LogProvider logProvider )
    {
        super( config, dbFactory, dependencies, logProvider );
    }

    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        return Arrays.asList(
                new DBMSModule( webServer ),
                new RESTApiModule( webServer, database, getConfig(), getDependencyResolver(), logProvider ),
                new ManagementApiModule( webServer, getConfig() ),
                new ThirdPartyJAXRSModule( webServer, getConfig(), logProvider, this ),
                new WebAdminModule( webServer, getConfig() ),
                new Neo4jBrowserModule( webServer ),
                new AuthorizationModule( webServer, authManager, logProvider, getConfig(), getUriWhitelist() ),
                new SecurityRulesModule( webServer, getConfig(), logProvider ) );
    }

    @Override
    protected WebServer createWebServer()
    {
		return new Jetty9WebServer( logProvider, getConfig() );
    }

    @Override
    public Iterable<AdvertisableService> getServices()
    {
        List<AdvertisableService> toReturn = new ArrayList<>( 3 );
        toReturn.add( new ConsoleService( null, null, logProvider, null ) );
        toReturn.add( new JmxService( null, null ) );
        toReturn.add( new MonitorService( null, null ) );

        return toReturn;
    }
}
