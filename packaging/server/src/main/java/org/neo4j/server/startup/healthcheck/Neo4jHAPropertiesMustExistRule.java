/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.startup.healthcheck;

import java.io.File;
import java.util.Properties;

import org.neo4j.server.configuration.Configurator;

public class Neo4jHAPropertiesMustExistRule extends Neo4jPropertiesMustExistRule
{
    @Override
    protected boolean validateProperties( Properties configProperties )
    {
        String dbTuningFilename = configProperties.getProperty( Configurator.DB_TUNING_PROPERTY_FILE_KEY );
        if ( dbTuningFilename == null )
        {
            failureMessage = String.format( "High-Availability mode requires %s to be set in %s",
                    Configurator.DB_TUNING_PROPERTY_FILE_KEY, Configurator.NEO_SERVER_CONFIG_FILE_KEY );
            return false;
        }
        else
        {
            File dbTuningFile = new File( dbTuningFilename );
            if ( !dbTuningFile.exists() )
            {
                failureMessage = String.format( "No database tuning file at [%s]", dbTuningFile.getAbsoluteFile() );
                return false;
            }
        }
        return true;
    }
}
