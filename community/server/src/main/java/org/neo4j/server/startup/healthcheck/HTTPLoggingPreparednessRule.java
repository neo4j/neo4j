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

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.neo4j.server.configuration.Configurator;

public class HTTPLoggingPreparednessRule implements StartupHealthCheckRule
{
    private String failureMessage = "";

    @Override
    public boolean execute( Properties properties )
    {
        boolean enabled = new Boolean(
            String.valueOf( properties.getProperty( Configurator.HTTP_LOGGING ) ) ).booleanValue();

        if ( !enabled )
        {
            return true;
        }

        File logLocation = new File( String.valueOf( properties.getProperty( Configurator.HTTP_LOG_LOCATION ) ) );

        boolean logLocationSuitable = true;

        try
        {
            FileUtils.forceMkdir(logLocation);
        }
        catch ( IOException e )
        {
            logLocationSuitable = false;
        }

        if ( !logLocation.exists() )
        {
            failureMessage = String.format( "HTTP log directory [%s] cannot be created",
                logLocation.getAbsolutePath() );
            return false;
        }

        if ( !logLocationSuitable )
        {
            failureMessage = String.format( "HTTP log directory [%s] does not exist", logLocation.getAbsolutePath() );
            return false;
        }
        else
        {
            logLocationSuitable = logLocation.canWrite();
        }

        if ( !logLocationSuitable )
        {
            failureMessage = String.format( "HTTP log directory [%s] is not writable", logLocation.getAbsolutePath() );
            return false;
        }

        return true;
    }

    @Override
    public String getFailureMessage()
    {
        return failureMessage;
    }
}
