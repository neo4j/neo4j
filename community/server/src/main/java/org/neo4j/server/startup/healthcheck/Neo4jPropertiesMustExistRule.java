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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import org.neo4j.server.configuration.Configurator;

public class Neo4jPropertiesMustExistRule implements StartupHealthCheckRule
{
    private static final String EMPTY_STRING = "";
    private boolean passed = false;
    private boolean ran = false;
    protected String failureMessage = EMPTY_STRING;

    public boolean execute( Properties properties )
    {
        ran = true;

        String configFilename = properties.getProperty( Configurator.NEO_SERVER_CONFIG_FILE_KEY );

        Properties configProperties = new Properties();
        FileInputStream inputStream = null;
        try
        {
            inputStream = new FileInputStream( configFilename );
            configProperties.load( inputStream );
        }
        catch ( IOException e )
        {
            failureMessage = String.format( "Failed to load configuration properties from [%s]", configFilename );
            return false;
        }
        finally
        {
            if ( inputStream != null )
            {
                try
                {
                    inputStream.close();
                }
                catch ( IOException e )
                { // Couldn't close it
                }
            }
        }

        passed = validateProperties( configProperties );
        return passed;
    }

    protected boolean validateProperties( Properties configProperties )
    {
        // default implementation: all OK
        return true;
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
