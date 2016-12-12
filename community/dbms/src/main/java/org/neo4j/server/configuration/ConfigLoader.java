/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConfigurationValidator;
import org.neo4j.kernel.configuration.Connector;
import org.neo4j.kernel.configuration.Settings;

public class ConfigLoader
{
    public static final String DEFAULT_CONFIG_FILE_NAME = "neo4j.conf";

    public static Config loadConfig( Optional<File> configFile, Pair<String,String>... configOverrides ) throws
            IOException
    {
        return loadConfig( Optional.empty(), configFile, configOverrides );
    }

    public static Config loadConfig( Optional<File> homeDir, Optional<File> configFile,
            Pair<String,String>... configOverrides )
    {
        Map<String,String> overriddenSettings = calculateSettings( homeDir, configOverrides );
        return Config.embeddedDefaults(configFile, overriddenSettings );
    }

    public static Config loadServerConfig( Optional<File> configFile, Pair<String,String>... configOverrides )
            throws IOException
    {
        return loadServerConfig( Optional.empty(), configFile, configOverrides, Collections.emptyList() );
    }

    public static Config loadServerConfig( Optional<File> homeDir, Optional<File> configFile,
            Pair<String,String>[] configOverrides, Collection<ConfigurationValidator> additionalValidators )
    {
        Map<String,String> overriddenSettings = calculateSettings( homeDir, configOverrides );
        return Config.serverDefaults( configFile, overriddenSettings, additionalValidators );
    }

    public static Config loadConfigWithConnectorsDisabled( Optional<File> homeDir, Optional<File> configFile,
            Pair<String,String>... configOverrides )
    {
        Map<String,String> overriddenSettings = calculateSettings( homeDir, configOverrides );
        return disableAllConnectors( Config.embeddedDefaults( configFile, overriddenSettings ) );
    }

    private static Map<String, String> calculateSettings( Optional<File> homeDir,
            Pair<String, String>[] configOverrides )
    {
        HashMap<String, String> settings = new HashMap<>();
        settings.putAll( toMap( configOverrides ) );
        settings.put( GraphDatabaseSettings.neo4j_home.name(),
                homeDir.map( File::getAbsolutePath ).orElse( System.getProperty( "user.dir" ) ) );
        return settings;
    }

    private static Map<String, String> toMap( Pair<String, String>[] configOverrides )
    {
        Map<String, String> overrides = new HashMap<>();
        for ( Pair<String, String> configOverride : configOverrides )
        {
            overrides.put( configOverride.first(), configOverride.other() );
        }
        return overrides;
    }

    private static Config disableAllConnectors( Config config )
    {
        return config.with(
                config.allConnectorIdentifiers().stream()
                        .collect( Collectors.toMap( id -> new Connector( id ).enabled.name(),
                                id -> Settings.FALSE ) ) );
    }
}
