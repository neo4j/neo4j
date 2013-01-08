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
package org.neo4j.server.startup.healthcheck;

import java.io.File;
import java.util.Properties;

import org.neo4j.server.configuration.Configurator;

public class ConfigFileMustBePresentRule implements StartupHealthCheckRule
{
    private static final String EMPTY_STRING = "";
    private boolean passed = false;
    private boolean ran = false;
    private String failureMessage = EMPTY_STRING;

    public boolean execute( Properties properties )
    {
        ran = true;

        String configFilename = properties.getProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY );

        if ( configFilename == null )
        {
            failureMessage = String.format( "Property [%s] has not been set.", Configurator.NEO_SERVER_CONFIG_FILE_KEY );

            return false;
        }

        File configFile = new File( configFilename );
        if ( !configFile.exists() )
        {
            failureMessage = String.format( "No configuration file at [%s]", configFile.getAbsoluteFile() );
            return false;
        }

        passed = true;
        return passed;
    }

    public String getFailureMessage()
    {
        if ( passed )
        {
            return EMPTY_STRING;
        }

        if ( !ran )
        {
            return String.format( "%s has not been run", getClass().getName() );
        }
        else
        {
            return failureMessage;
        }
    }
}
