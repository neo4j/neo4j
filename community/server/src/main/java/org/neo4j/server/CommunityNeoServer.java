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

import java.util.Arrays;
import java.util.Collections;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.server.database.CommunityGraphFactory;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.ManagementApiModule;
import org.neo4j.server.modules.Neo4jBrowserModule;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.SecurityRulesModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.rest.discovery.CommunityDiscoverableURIs.communityDiscoverableURIs;

public class CommunityNeoServer extends AbstractNeoServer
{
    public CommunityNeoServer( Config config, ExternalDependencies dependencies )
    {
        this( config, new CommunityGraphFactory(), dependencies );
    }

    public CommunityNeoServer( Config config, GraphFactory graphFactory, ExternalDependencies dependencies )
    {
        super( config, graphFactory, dependencies );
    }

    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        return Arrays.asList(
                createDBMSModule(),
                new RESTApiModule( webServer, getConfig(), userLogProvider ),
                new ManagementApiModule( webServer, getConfig() ),
                new ThirdPartyJAXRSModule( webServer, getConfig(), userLogProvider, this ),
                new Neo4jBrowserModule( webServer ),
                createAuthorizationModule(),
                new SecurityRulesModule( webServer, getConfig(), userLogProvider ) );
    }

    @Override
    protected WebServer createWebServer()
    {
        NetworkConnectionTracker connectionTracker = getDependencyResolver().resolveDependency( NetworkConnectionTracker.class );
        return new Jetty9WebServer( userLogProvider, getConfig(), connectionTracker );
    }

    @Override
    public Iterable<AdvertisableService> getServices()
    {
        return Collections.emptyList();
    }

    protected DBMSModule createDBMSModule()
    {
        // ConnectorPortRegister isn't available until runtime, so defer loading until then
        Supplier<DiscoverableURIs> discoverableURIs  = () -> communityDiscoverableURIs( getConfig(),
                getDependencyResolver().resolveDependency( ConnectorPortRegister.class ) );
        return new DBMSModule( webServer, getConfig(), discoverableURIs );
    }

    protected AuthorizationModule createAuthorizationModule()
    {
        return new AuthorizationModule( webServer, authManagerSupplier, userLogProvider, getConfig(), getUriWhitelist() );
    }
}
