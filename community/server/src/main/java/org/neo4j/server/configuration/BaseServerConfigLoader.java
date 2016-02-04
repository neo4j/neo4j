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
    public Config loadConfig( File configFile, File legacyConfigFile, Log log, Pair<String, String> ... configOverrides )
    {
        if ( log == null )
        {
            throw new IllegalArgumentException( "log cannot be null ");
        }

        Config config = new Config();
        config.setLogger( log );

        // For now, don't print warnings if this file is not specified
        if( configFile != null && configFile.exists() )
        {
            config.augment( loadFromFile( log, configFile ) );
        }

        config.augment( loadFromFile( log, legacyConfigFile ) );

        overrideEmbeddedDefaults( config );
        applyUserOverrides( config, configOverrides );

        config.registerSettingsClasses( getDefaultSettingsClasses() );

        return config;
    }

    private void applyUserOverrides( Config config, Pair<String,String>[] configOverrides )
    {
        Map<String,String> params = config.getParams();
        for ( Pair<String,String> configOverride : configOverrides )
        {
            params.put( configOverride.first(), configOverride.other() );
        }
        config.applyChanges( params );
    }

    /**
     * This is a smell - this means docs will say defaults are something other than what they are in the server. Better make embedded the special case and
     * set the defaults to be what the server will have.
     *
     * In any case - this overrides embedded defaults.
     */
    private static void overrideEmbeddedDefaults( Config config )
    {
        Map<String,String> params = config.getParams();
        if ( !params.containsKey( ShellSettings.remote_shell_enabled.name() ) )
        {
            params.put( ShellSettings.remote_shell_enabled.name(), TRUE );
        }

        if ( !params.containsKey( GraphDatabaseSettings.log_queries_filename.name() ) )
        {
            params.put( GraphDatabaseSettings.log_queries_filename.name(), "data/log/queries.log" );
        }

        if( !params.containsKey( BoltKernelExtension.Settings.enabled.name() ))
        {
            params.put( BoltKernelExtension.Settings.enabled.name(), "true" );
        }

        config.applyChanges( params );
    }

    protected static Iterable<Class<?>> getDefaultSettingsClasses()
    {
        return asList( ServerSettings.class, GraphDatabaseSettings.class );
    }

    private static Map<String,String> loadFromFile( Log log, File file )
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
