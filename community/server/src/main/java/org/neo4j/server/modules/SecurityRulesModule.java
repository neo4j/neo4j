/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.ArrayList;

import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.logging.ConsoleLogger;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.rest.security.SecurityFilter;
import org.neo4j.server.rest.security.SecurityRule;
import org.neo4j.server.web.WebServer;

public class SecurityRulesModule implements ServerModule
{
    private final WebServer webServer;
    private final Config config;
    private final ConsoleLogger log;

    private SecurityFilter mountedFilter;

    public SecurityRulesModule( WebServer webServer, Config config, Logging logging )
    {
        this.webServer = webServer;
        this.config = config;
        this.log = logging.getConsoleLog( getClass() );
    }

    @Override
    public void start()
    {
        Iterable<SecurityRule> securityRules = getSecurityRules();
        if ( Iterables.count( securityRules ) > 0 )
        {
            mountedFilter = new SecurityFilter( securityRules );

            webServer.addFilter( mountedFilter, "/*" );

            for ( SecurityRule rule : securityRules )
            {
                log.log( "Security rule [%s] installed on server",
                        rule.getClass().getCanonicalName() );
            }
        }
    }

    @Override
    public void stop()
    {
        if ( mountedFilter != null )
        {
            mountedFilter.destroy();
        }
    }

    private Iterable<SecurityRule> getSecurityRules()
    {
        ArrayList<SecurityRule> rules = new ArrayList<SecurityRule>();

        for ( String classname : config.get( ServerSettings.security_rules ) )
        {
            try
            {
                rules.add( (SecurityRule) Class.forName( classname ).newInstance() );
            }
            catch ( Exception e )
            {
                log.error( "Could not load server security rule [%s], exception details: ", classname, e.getMessage() );
                e.printStackTrace();
            }
        }

        return rules;
    }
}
