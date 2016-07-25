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
package org.neo4j.server;

import java.io.File;
import java.util.Collection;
import java.util.Optional;

import org.neo4j.helpers.Args;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.impl.util.Converters;
import org.neo4j.server.configuration.ConfigLoader;

import static org.neo4j.helpers.collection.Pair.pair;

/**
 * Parses command line arguments for the server bootstrappers. Format is as follows:
 * <ul>
 * <li>Configuration file can be specified by <strong>--config=path/to/config.properties</strong> or
 * <strong>-C=path/to/config.properties</strong></li>
 * <li>Specific overridden configuration options, directly specified as arguments can be specified with
 * <strong>-c key=value</strong>, for example <strong>-c dbms.active_database=foo.db</strong>
 * or enabled boolean properties with <strong>-c key</strong>, f.ex <strong>-c dbms.readonly</strong>
 * </ul>
 */
public class ServerCommandLineArgs
{
    public static final String CONFIG_DIR_ARG = "config-dir";
    public static final String HOME_DIR_ARG = "home-dir";
    private final Args args;
    private final Pair<String, String>[] configOverrides;

    private ServerCommandLineArgs( Args args, Pair<String, String>[] configOverrides )
    {
        this.args = args;
        this.configOverrides = configOverrides;
    }

    public static ServerCommandLineArgs parse( String[] argv )
    {
        Args args = Args.parse( argv );
        return new ServerCommandLineArgs( args, parseConfigOverrides( args ) );
    }

    public Pair<String, String>[] configOverrides()
    {
        return configOverrides;
    }

    public Optional<File> configFile()
    {
        return Optional.ofNullable( args.get( CONFIG_DIR_ARG ) )
                .map( (dirPath) -> new File( dirPath, ConfigLoader.DEFAULT_CONFIG_FILE_NAME ) );
    }

    private static Pair<String, String>[] parseConfigOverrides( Args arguments )
    {
        Collection<Pair<String, String>> options = arguments.interpretOptions( "c",
                Converters.<Pair<String, String>>optional(), s -> {
                    if ( s.contains( "=" ) )
                    {
                        String[] keyVal = s.split( "=", 2 );
                        return pair( keyVal[0], keyVal[1] );
                    }
                    // Shortcut to specify boolean flags ("-c dbms.enableTheFeature")
                    return pair( s, "true" );
                } );

        return options.toArray( new Pair[options.size()] );
    }

    public File homeDir()
    {
        if ( args.get( HOME_DIR_ARG ) == null )
        {
            return null;
        }

        return new File( args.get( HOME_DIR_ARG ) );
    }
}
