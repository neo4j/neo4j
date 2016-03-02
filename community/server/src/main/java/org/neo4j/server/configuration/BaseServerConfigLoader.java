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
import java.util.Map;

import org.neo4j.bolt.BoltKernelExtension;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.shell.ShellSettings;

import static java.util.Arrays.asList;

import static org.neo4j.kernel.configuration.Settings.TRUE;

public class BaseServerConfigLoader
{
    public Config loadConfig( File configFile, File legacyConfigFile, Log log, Pair<String, String>... configOverrides )
    {
        if ( log == null )
        {
            throw new IllegalArgumentException( "log cannot be null " );
        }

        HashMap<String, String> settings = calculateSettings( configFile, legacyConfigFile, log, configOverrides );
        Config config = new Config( settings, settingsClasses(settings) );
        config.setLogger( log );
        return config;
    }

    protected Iterable<Class<?>> settingsClasses( HashMap<String, String> settings )
    {
        return asList( ServerSettings.class, GraphDatabaseSettings.class, DatabaseManagementSystemSettings.class );
    }

    private HashMap<String, String> calculateSettings( File configFile, File legacyConfigFile, Log log,
                                                       Pair<String, String>[] configOverrides )
    {
        HashMap<String, String> settings = new HashMap<>();
        if ( configFile != null && configFile.exists() )
        {
            settings.putAll( loadFromFile( log, configFile ) );
        }
        settings.putAll( loadFromFile( log, legacyConfigFile ) );
        settings.putAll( toMap( configOverrides ) );
        overrideEmbeddedDefaults( settings );
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
    private static void overrideEmbeddedDefaults( HashMap<String, String> config )
    {
        config.putIfAbsent( ShellSettings.remote_shell_enabled.name(), TRUE );
        config.putIfAbsent( GraphDatabaseSettings.log_queries_filename.name(), "data/log/queries.log" );
        config.putIfAbsent( BoltKernelExtension.Settings.enabled.name(), "true" );
        config.putIfAbsent( GraphDatabaseSettings.auth_enabled.name(), "true" );
    }

    private static Map<String, String> loadFromFile( Log log, File file )
    {
        if ( file == null )
        {
            return new HashMap<>();
        }

        if ( file.exists() )
        {
            try
            {
                return MapUtil.load( file );
            }
            catch ( IOException e )
            {
                log.error( "Unable to load config file [%s]: %s", file, e.getMessage() );
            }
        }
        else
        {
            log.warn( "Config file [%s] does not exist.", file );
        }

        // Default to no user-defined config if no config was found
        return new HashMap<>();
    }
}
