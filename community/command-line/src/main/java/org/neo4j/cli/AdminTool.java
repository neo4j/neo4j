/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

package org.neo4j.cli;

import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ExitCode;
import picocli.CommandLine.HelpCommand;

import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.kernel.internal.Version;
import org.neo4j.service.Services;
import org.neo4j.util.VisibleForTesting;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.neo4j.cli.AdminTool.VersionProvider;
import static picocli.CommandLine.IVersionProvider;

@Command(
        name = "neo4j-admin",
        description = "Neo4j database administration tool.",
        mixinStandardHelpOptions = true,
        versionProvider = VersionProvider.class
)
public final class AdminTool
{
    private AdminTool()
    {
        // nope
    }

    public static void main( String[] args )
    {
        final var homeDir = getDirOrExit( "NEO4J_HOME" );
        final var confDir = getDirOrExit( "NEO4J_CONF" );
        final var ctx = new ExecutionContext( homeDir, confDir );
        final var exitCode = execute( ctx, args );
        System.exit( exitCode );
    }

    @SuppressWarnings( "InstantiationOfUtilityClass" )
    @VisibleForTesting
    public static int execute( ExecutionContext ctx, String... args )
    {
        final var cmd = new CommandLine( new AdminTool() )
            .setUsageHelpWidth( 120 )
            .setCaseInsensitiveEnumValuesAllowed( true );
        registerCommands( cmd, ctx );

        if ( args.length == 0 )
        {
            cmd.usage( System.out );
            return ExitCode.USAGE;
        }

        return cmd.execute( args );
    }

    private static void registerCommands( CommandLine cmd, ExecutionContext ctx )
    {
        cmd.addSubcommand( HelpCommand.class );
        final var providers = Services.loadAll( CommandProvider.class );
        for ( final CommandProvider provider : providers )
        {
            cmd.addSubcommand( provider.createCommand( ctx ) );
        }
    }

    private static Path getDirOrExit( String envVar )
    {
        final var value = System.getenv( envVar );
        if ( isBlank( value ) )
        {
            System.err.printf( "Required environment variable '%s' is not set%n", envVar );
            System.exit( ExitCode.USAGE );
        }
        final var path = Path.of( value ).toAbsolutePath();
        if ( !Files.isDirectory( path ) )
        {
            System.err.printf( "%s path doesn't exist or not a directory: %s%n", envVar, path );
            System.exit( ExitCode.USAGE );
        }
        return path;
    }

    static class VersionProvider implements IVersionProvider
    {
        @Override
        public String[] getVersion()
        {
            return new String[]{Version.getNeo4jVersion()};
        }
    }
}
