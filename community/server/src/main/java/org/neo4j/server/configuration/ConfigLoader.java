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
import java.util.function.Consumer;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.shell.ShellSettings;

import static org.neo4j.dbms.DatabaseManagementSystemSettings.data_directory;
import static org.neo4j.kernel.configuration.Settings.TRUE;

public class ConfigLoader
{
    public static final String DEFAULT_CONFIG_FILE_NAME = "neo4j.conf";

    private final SettingsClasses settingsClasses;

    public ConfigLoader( SettingsClasses settingsClasses )
    {
        this.settingsClasses = settingsClasses;
    }

    public ConfigLoader( List<Class<?>> settingsClasses )
    {
        this( settings -> settingsClasses );
    }

    public Config loadConfig( Optional<File> configFile, Log log, Pair<String, String>... configOverrides )
    {
        return loadConfig( configFile, log, settings -> {}, configOverrides );
    }

    public Config loadConfig( Optional<File> configFile,
                              Log log,
                              Consumer<Map<String, String>> customizer,
                              Pair<String, String>... configOverrides )
    {
        if ( log == null )
        {
            throw new IllegalArgumentException( "log cannot be null" );
        }

        Map<String, String> settings = calculateSettings( configFile, log, configOverrides, customizer );
        Config config = new Config( settings, settingsClasses.calculate( settings ) );
        config.setLogger( log );
        return config;
    }


    private Map<String, String> calculateSettings( Optional<File> config, Log log,
                                                   Pair<String, String>[] configOverrides,
                                                   Consumer<Map<String, String>> customizer )
    {
        HashMap<String, String> settings = new HashMap<>();

        config.ifPresent( ( c ) -> settings.putAll( loadFromFile( log, c ) ) );
        settings.putAll( toMap( configOverrides ) );
        overrideEmbeddedDefaults( settings );
        settings.put( GraphDatabaseSettings.neo4j_home.name(), System.getProperty( "user.dir" ) );
        customizer.accept( settings );
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
        config.putIfAbsent( ShellSettings.remote_shell_enabled.name(), TRUE );
        config.putIfAbsent( GraphDatabaseSettings.auth_enabled.name(), "true" );

        String dataDirectory = config.getOrDefault( data_directory.name(), data_directory.getDefaultValue() );
        config.putIfAbsent( GraphDatabaseSettings.auth_store.name(),
                new File( dataDirectory, "dbms/auth" ).toString() );
    }

    private static Map<String, String> loadFromFile( Log log, File file )
    {
        if ( !file.exists() )
        {
            log.warn( "Config file [%s] does not exist.", file );
            return new HashMap<>();
        }

        try
        {
            return MapUtil.load( file );
        }
        catch ( IOException e )
        {
            log.error( "Unable to load config file [%s]: %s", file, e.getMessage() );
            return new HashMap<>();
        }
    }

    public interface SettingsClasses
    {
        Iterable<Class<?>> calculate( Map<String, String> settings );
    }
}
