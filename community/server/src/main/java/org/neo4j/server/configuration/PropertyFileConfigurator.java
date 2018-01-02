/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.configuration;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.server.web.ServerInternalSettings;


/**
 * @deprecated this will be removed in the next major version of Neo4j
 */
public class PropertyFileConfigurator implements ConfigurationBuilder
{
    private final Config serverConfig;
    private Map<String,String> databaseTuningProperties;
    private Map<String,String> serverProperties;

    public PropertyFileConfigurator( File propertiesFile, Log log )
    {
        if ( propertiesFile == null )
        {
            throw new IllegalArgumentException( "propertiesFile cannot be null ");
        }
        if ( log == null )
        {
            throw new IllegalArgumentException( "log cannot be null ");
        }

        loadServerProperties( propertiesFile, log );
        loadDatabaseTuningProperties( propertiesFile, log );

        serverConfig = new Config( serverProperties, BaseServerConfigLoader.getDefaultSettingsClasses() );
    }

    @Override
    public Config configuration()
    {
        return serverConfig == null ? new Config() : serverConfig;
    }

    @Override
    public Map<String,String> getDatabaseTuningProperties()
    {
        return databaseTuningProperties == null ? new HashMap<String,String>() : databaseTuningProperties;
    }

    private void loadDatabaseTuningProperties( File configFile, Log log )
    {
        String databaseTuningPropertyPath = serverProperties.get( ServerInternalSettings.legacy_db_config.name() );
        if ( databaseTuningPropertyPath == null )
        {
            // try to find the db config file
            databaseTuningPropertyPath =
                    configFile.getParent() + File.separator + ServerInternalSettings.DB_TUNING_CONFIG_FILE_NAME;
            serverProperties.put( ServerInternalSettings.legacy_db_config.name(), databaseTuningPropertyPath );
            log.warn( "No database tuning file explicitly set, defaulting to [%s]", databaseTuningPropertyPath );
        }

        File databaseTuningPropertyFile = new File( databaseTuningPropertyPath );
        if ( !databaseTuningPropertyFile.exists() )
        {
            log.warn( "The specified file for database performance tuning properties [%s] does not exist.",
                    databaseTuningPropertyPath );
        }
        else
        {
            try
            {
                databaseTuningProperties = MapUtil.load( databaseTuningPropertyFile );
            }
            catch ( IOException e )
            {
                log.warn( "Unable to load database tuning file: " + e.getMessage() );
            }
        }
        // Default to no user-defined config if no config was found
        if ( databaseTuningProperties == null )
        {
            databaseTuningProperties = new HashMap<>();
        }
    }

    private void loadServerProperties( File serverConfigFile, Log log )
    {
        if ( serverConfigFile == null )
        {
            // load the server config file from the default location.
            serverConfigFile = new File( System.getProperty(
                    ServerInternalSettings.SERVER_CONFIG_FILE_KEY, ServerInternalSettings.SERVER_CONFIG_FILE ) );
        }

        if ( !serverConfigFile.exists() )
        {
            log.warn( "The specified file for server configuration [%s] does not exist. " +
                      "Using the default non-user-defined server configuration.",
                    serverConfigFile.getAbsoluteFile() );
        }
        else
        {
            try
            {
                serverProperties = MapUtil.load( serverConfigFile );
            }
            catch ( IOException e )
            {
                log.warn( "Unable to load server configuration file: " + e.getMessage() );
            }
        }
        // Default to no user-defined config if no config was found
        if ( serverProperties == null )
        {
            serverProperties = new HashMap<>();
        }
    }
}
