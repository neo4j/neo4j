/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.startup.healthcheck;

import java.util.Properties;

import org.neo4j.server.logging.Logger;

public class StartupHealthCheck
{
    public static final Logger log = Logger.getLogger( StartupHealthCheck.class );

    private final StartupHealthCheckRule[] rules;

    private StartupHealthCheckRule failedRule = null;

    public StartupHealthCheck( StartupHealthCheckRule... rules )
    {
        this.rules = rules;
    }

    public boolean run()
    {
        if ( rules == null || rules.length < 1 )
        {
            return true;
        }

        Properties properties = System.getProperties();
        for ( StartupHealthCheckRule r : rules )
        {
            if ( !r.execute( properties ) )
            {
                log.error( r.getFailureMessage() );
                failedRule = r;
                return false;
            }
        }

        return true;
    }

    public StartupHealthCheckRule failedRule()
    {
        return failedRule;
    }
}
