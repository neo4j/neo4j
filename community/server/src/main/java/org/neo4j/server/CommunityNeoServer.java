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
package org.neo4j.server;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.CommunityEditionModule;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.ConsoleModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.ManagementApiModule;
import org.neo4j.server.modules.Neo4jBrowserModule;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.SecurityRulesModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.management.JmxService;
import org.neo4j.server.rest.management.console.ConsoleService;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class CommunityNeoServer extends AbstractNeoServer
{
    protected static final GraphFactory COMMUNITY_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        return new GraphDatabaseFacadeFactory( DatabaseInfo.COMMUNITY, CommunityEditionModule::new )
                .newFacade( storeDir, config, dependencies );
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
                new DBMSModule( webServer, getConfig() ),
                new RESTApiModule( webServer, getConfig(), getDependencyResolver(), logProvider ),
                new ManagementApiModule( webServer, getConfig() ),
                new ThirdPartyJAXRSModule( webServer, getConfig(), logProvider, this ),
                new ConsoleModule( webServer, getConfig() ),
                new Neo4jBrowserModule( webServer ),
                createAuthorizationModule(),
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

        return toReturn;
    }

    protected AuthorizationModule createAuthorizationModule()
    {
        return new AuthorizationModule( webServer, authManagerSupplier, logProvider, getConfig(), getUriWhitelist() );
    }
}
