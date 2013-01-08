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
package org.neo4j.server;

import java.util.Arrays;

import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.database.CommunityDatabase;
import org.neo4j.server.database.Database;
import org.neo4j.server.modules.DiscoveryModule;
import org.neo4j.server.modules.ManagementApiModule;
import org.neo4j.server.modules.RESTApiModule;
import org.neo4j.server.modules.SecurityRulesModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.modules.StatisticModule;
import org.neo4j.server.modules.ThirdPartyJAXRSModule;
import org.neo4j.server.modules.WebAdminModule;
import org.neo4j.server.startup.healthcheck.ConfigFileMustBePresentRule;
import org.neo4j.server.startup.healthcheck.HTTPLoggingPreparednessRule;
import org.neo4j.server.startup.healthcheck.Neo4jPropertiesMustExistRule;
import org.neo4j.server.startup.healthcheck.StartupHealthCheck;
import org.neo4j.server.web.Jetty6WebServer;
import org.neo4j.server.web.WebServer;

public class CommunityNeoServer extends AbstractNeoServer
{

    protected CommunityNeoServer()
    {
        
    }
    
    public CommunityNeoServer( Configurator configurator )
    {
        this.configurator = configurator;
        init();
    }

	@Override
	protected StartupHealthCheck createHealthCheck() {
		return new StartupHealthCheck(
				new ConfigFileMustBePresentRule(), 
				new Neo4jPropertiesMustExistRule(),
				new HTTPLoggingPreparednessRule() );
	}

	@Override
	protected Iterable<ServerModule> createServerModules() 
	{
        return Arrays.asList( 
        		new DiscoveryModule(webServer), 
        		new RESTApiModule(webServer, database, configurator.configuration()), 
        		new ManagementApiModule(webServer, configurator.configuration()),
                new ThirdPartyJAXRSModule(webServer, configurator), 
                new WebAdminModule(webServer, configurator.configuration(), database), 
                new StatisticModule(webServer, statisticsCollector, configurator.configuration()),
                new SecurityRulesModule(webServer, configurator.configuration()));
	}

	@Override
	protected Database createDatabase() {
		return new CommunityDatabase(configurator.configuration());
	}

	@Override
	protected WebServer createWebServer() {
		return new Jetty6WebServer();
	}
}
