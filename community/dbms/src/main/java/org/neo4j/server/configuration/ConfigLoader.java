/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;

public class ConfigLoader
{
    public static final String DEFAULT_CONFIG_FILE_NAME = "neo4j.conf";

    private final Function<Map<String, String> ,Iterable<Class<?>>> settingsClassesSupplier;

    public ConfigLoader( Function<Map<String, String> ,Iterable<Class<?>>> settingsClassesSupplier )
    {
        this.settingsClassesSupplier = settingsClassesSupplier;
    }

    public ConfigLoader( List<Class<?>> settingsClasses )
    {
        this( settings -> settingsClasses );
    }

    public Config loadConfig( Optional<File> configFile, Pair<String,String>... configOverrides ) throws IOException
    {
        return loadConfig( Optional.empty(), configFile, configOverrides );
    }

    public Config loadConfig( Optional<File> homeDir, Optional<File> configFile,
            Pair<String,String>... configOverrides )
    {
        Map<String,String> overriddenSettings = calculateSettings( homeDir, configOverrides );
        return new Config( configFile, overriddenSettings, ConfigLoader::overrideEmbeddedDefaults,
                settingsClassesSupplier );
    }

    private Map<String, String> calculateSettings( Optional<File> homeDir,
            Pair<String, String>[] configOverrides )
    {
        HashMap<String, String> settings = new HashMap<>();
        settings.putAll( toMap( configOverrides ) );
        settings.put( GraphDatabaseSettings.neo4j_home.name(),
                homeDir.map( File::getAbsolutePath ).orElse( System.getProperty( "user.dir" ) ) );
        return settings;
    }

    private Map<String, String> toMap( Pair<String, String>[] configOverrides )
    {
        Map<String, String> overrides = new HashMap<>();
        for ( Pair<String, String> configOverride : configOverrides )
        {
            overrides.put( configOverride.first(), configOverride.other() );
        }
        return overrides;
    }

    /*
     * TODO: This means docs will say defaults are something other than what they are in the server. Better
     * make embedded the special case and set the defaults to be what the server will have.
     */
    private static void overrideEmbeddedDefaults( Map<String, String> config )
    {
        config.putIfAbsent( GraphDatabaseSettings.auth_enabled.name(), "true" );
    }
}
