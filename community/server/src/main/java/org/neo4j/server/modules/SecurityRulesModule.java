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

import java.util.ArrayList;

import org.apache.commons.configuration.Configuration;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.logging.Logger;
import org.neo4j.server.rest.security.SecurityFilter;
import org.neo4j.server.rest.security.SecurityRule;
import org.neo4j.server.web.WebServer;

public class SecurityRulesModule implements ServerModule
{
    private final Logger log = Logger.getLogger( SecurityRulesModule.class );

    private final WebServer webServer;
    private final Configuration config;

    private SecurityFilter mountedFilter;

    public SecurityRulesModule( WebServer webServer, Configuration config )
    {
        this.webServer = webServer;
        this.config = config;
    }

    @Override
    public void start( StringLogger logger )
    {
        Iterable<SecurityRule> securityRules = getSecurityRules();
        mountedFilter = new SecurityFilter( securityRules );

        webServer.addFilter( mountedFilter, "/*" );

        for ( SecurityRule rule : securityRules )
        {
            log.info( "Security rule [%s] installed on server",
                    rule.getClass().getCanonicalName() );
            System.out.println(
                    String.format( "Security rule [%s] installed on server", rule.getClass().getCanonicalName() ) );

        }
    }

    @Override
    public void stop()
    {
        mountedFilter.destroy();
    }

    private Iterable<SecurityRule> getSecurityRules()
    {
        ArrayList<SecurityRule> rules = new ArrayList<SecurityRule>();

        for ( String classname : config.getStringArray( Configurator.SECURITY_RULES_KEY ) )
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
