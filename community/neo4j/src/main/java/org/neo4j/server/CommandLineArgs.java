/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.nio.file.Path;
import java.util.Collection;
import java.util.Map;

import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.Args;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.impl.util.Converters;

import static java.lang.Boolean.FALSE;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
import static org.neo4j.internal.helpers.collection.Pair.pair;

/**
 * Parses command line arguments for the bootstrappers. Format is as follows:
 * <ul>
 * <li>Configuration file can be specified by <strong>--config=path/to/config.properties</strong> or
 * <strong>-C=path/to/config.properties</strong></li>
 * <li>Specific overridden configuration options, directly specified as arguments can be specified with
 * <strong>-c key=value</strong>, for example <strong>-c dbms.default_database=foo.db</strong>
 * or enabled boolean properties with <strong>-c key</strong>, f.ex <strong>-c dbms.readonly</strong>
 * </ul>
 */
public class CommandLineArgs
{
    public static final String CONFIG_DIR_ARG = "config-dir";
    public static final String HOME_DIR_ARG = "home-dir";
    public static final String VERSION_ARG = "version";
    public static final String EXPAND_COMMAND_ARG = "expand-commands";
    private final Args args;
    private final Map<String, String> configOverrides;

    private CommandLineArgs( Args args, Map<String, String> configOverrides )
    {
        this.args = args;
        this.configOverrides = configOverrides;
    }

    public static CommandLineArgs parse( String[] argv )
    {
        Args args = Args.withFlags( VERSION_ARG ).parse( argv );
        return new CommandLineArgs( args, parseConfigOverrides( args ) );
    }

    public Map<String, String> configOverrides()
    {
        return configOverrides;
    }

    public Path configFile()
    {
        String configDirectory = args.get( CONFIG_DIR_ARG );
        return configDirectory == null ? null : Path.of( configDirectory, Config.DEFAULT_CONFIG_FILE_NAME );
    }

    private static Map<String, String> parseConfigOverrides( Args arguments )
    {
        Collection<Pair<String, String>> options = arguments.interpretOptions( "c",
                Converters.optional(), s ->
                {
                    if ( s.contains( "=" ) )
                    {
                        String[] keyVal = s.split( "=", 2 );
                        return pair( keyVal[0], keyVal[1] );
                    }
                    // Shortcut to specify boolean flags ("-c dbms.enableTheFeature")
                    return pair( s, "true" );
                } );

        Map<String,String> ret = stringMap();
        options.forEach( pair -> ret.put( pair.first(), pair.other() ) );

        return ret;
    }

    public Path homeDir()
    {
        if ( args.get( HOME_DIR_ARG ) == null )
        {
            return null;
        }

        return Path.of( args.get( HOME_DIR_ARG ) );
    }

    public boolean version()
    {
        return args.getBoolean( VERSION_ARG, FALSE );
    }

    public boolean expandCommands()
    {
        return args.getBoolean( EXPAND_COMMAND_ARG, FALSE );
    }
}
