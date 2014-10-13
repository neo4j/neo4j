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
package org.neo4j.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.Database;
import org.neo4j.server.modules.DiscoveryModule;
import org.neo4j.server.modules.ManagementApiModule;
import org.neo4j.server.modules.Neo4jBrowserModule;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.SecurityRulesModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.modules.WebAdminModule;
import org.neo4j.server.preflight.EnsurePreparedForHttpLogging;
import org.neo4j.server.preflight.PerformRecoveryIfNecessary;
import org.neo4j.server.preflight.PerformUpgradeIfNecessary;
import org.neo4j.server.preflight.PreFlightTasks;
import org.neo4j.server.rest.management.AdvertisableService;
import org.neo4j.server.rest.management.JmxService;
import org.neo4j.server.rest.management.MonitorService;
import org.neo4j.server.rest.management.console.ConsoleService;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.database.LifecycleManagingDatabase.EMBEDDED;
import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

/**
 * @deprecated This class is for internal use only and will be moved to an internal package in a future release.
 * Please use Neo4j Server and plugins or un-managed extensions for bespoke solutions.
 */
@Deprecated
public class CommunityNeoServer extends AbstractNeoServer
{
    public CommunityNeoServer( Configurator configurator, Logging logging )
    {
        this( configurator, lifecycleManagingDatabase( EMBEDDED ), logging );
    }

    public CommunityNeoServer( Configurator configurator, Database.Factory dbFactory, Logging logging )
    {
        super( configurator, dbFactory, logging );
    }

    @Override
    protected PreFlightTasks createPreflightTasks()
    {
        return new PreFlightTasks( logging,
                // TODO: Move the config check into bootstrapper
                //new EnsureNeo4jPropertiesExist(configurator.configuration()),
                new EnsurePreparedForHttpLogging(configurator.configuration()),
                new PerformUpgradeIfNecessary(getConfiguration(),
                        configurator.getDatabaseTuningProperties(), logging, StoreUpgrader.NO_MONITOR),
                new PerformRecoveryIfNecessary(getConfiguration(),
                        configurator.getDatabaseTuningProperties(), System.out, logging));
    }

    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        return Arrays.asList(
                new DiscoveryModule(webServer, logging),
                new RESTApiModule(webServer, database, configurator.configuration(), logging),
                new ManagementApiModule(webServer, configurator.configuration(), logging),
                new ThirdPartyJAXRSModule(webServer, configurator, logging, this),
                new WebAdminModule(webServer, logging),
                new Neo4jBrowserModule(webServer, configurator.configuration(), logging, database),
                new SecurityRulesModule(webServer, configurator.configuration(), logging));
    }

    @Override
    protected WebServer createWebServer()
    {
        return new Jetty9WebServer( logging );
    }

    @Override
    public Iterable<AdvertisableService> getServices()
    {
        List<AdvertisableService> toReturn = new ArrayList<>( 3 );
        toReturn.add( new ConsoleService( null, null, logging, null ) );
        toReturn.add( new JmxService( null, null ) );
        toReturn.add( new MonitorService( null, null ) );

        return toReturn;
    }
}
