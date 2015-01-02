/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.server.enterprise;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.commons.configuration.Configuration;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.server.configuration.Configurator;
import org.neo4j.server.preflight.EnsureNeo4jPropertiesExist;

public class EnsureEnterpriseNeo4jPropertiesExist extends EnsureNeo4jPropertiesExist
{
    public static final String CONFIG_KEY_OLD_SERVER_ID = "ha.machine_id";
    public static final String CONFIG_KEY_OLD_COORDINATORS = "ha.zoo_keeper_servers";

    public EnsureEnterpriseNeo4jPropertiesExist( Configuration config )
    {
        super( config );
    }

    // TODO: This validation should be done by the settings classes in HA 
    // and by the enterprise server settings, once we have refactored them 
    // to use the new config scheme.
    @Override
    protected boolean validateProperties( Properties configProperties )
    {
        String dbMode = configProperties.getProperty( Configurator.DB_MODE_KEY,
                EnterpriseDatabase.DatabaseMode.SINGLE.name() );
        dbMode = dbMode.toUpperCase();
        if ( dbMode.equals( EnterpriseDatabase.DatabaseMode.SINGLE.name() ) )
        {
            return true;
        }
        if ( !dbMode.equals( EnterpriseDatabase.DatabaseMode.HA.name() ) )
        {
            failureMessage = String.format( "Illegal value for %s \"%s\" in %s", Configurator.DB_MODE_KEY, dbMode,
                    Configurator.NEO_SERVER_CONFIG_FILE_KEY );
            return false;
        }

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
            else
            {
                Properties dbTuning = new Properties();
                try
                {
                    InputStream tuningStream = new FileInputStream( dbTuningFile );
                    try
                    {
                        dbTuning.load( tuningStream );
                    }
                    finally
                    {
                        tuningStream.close();
                    }
                }
                catch ( IOException e )
                {
                    // Shouldn't happen, we already covered those cases
                    failureMessage = e.getMessage();
                    return false;
                }
                String machineId = null;
                try
                {
                    machineId = getSinglePropertyFromCandidates( dbTuning, ClusterSettings.server_id.name(),
                            CONFIG_KEY_OLD_SERVER_ID, "<not set>" );
                    if ( Integer.parseInt( machineId ) < 0 )
                    {
                        throw new NumberFormatException();
                    }
                }
                catch ( NumberFormatException e )
                {
                    failureMessage = String.format( "%s in %s needs to be a non-negative integer, not %s",
                            ClusterSettings.server_id.name(), dbTuningFilename, machineId );
                    return false;
                }
                catch ( IllegalArgumentException e )
                {
                    failureMessage = String.format( "%s in %s", e.getMessage(), dbTuningFilename );
                    return false;
                }
            }
        }
        return true;
    }

    private String getSinglePropertyFromCandidates( Properties dbTuning, String first,
                                                    String other, String defaultValue )
    {
        String firstValue = dbTuning.getProperty( first );
        String otherValue = dbTuning.getProperty( other );
        if ( firstValue == null && otherValue == null )
        {
            return defaultValue;
        }
        // Perhaps not a correct use of IllegalArgumentException
        if ( firstValue != null && otherValue != null )
        {
            throw new IllegalArgumentException( "Multiple configuration values set for the same logical property [" +
                    first + "," + other + "]" );
        }
        return firstValue != null ? firstValue : otherValue;
    }
}
