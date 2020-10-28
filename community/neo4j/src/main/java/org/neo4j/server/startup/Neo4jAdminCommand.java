/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.startup;

import picocli.CommandLine;

import java.io.PrintStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.Function;

import org.neo4j.cli.AdminTool;
import org.neo4j.configuration.BootloaderSettings;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.util.VisibleForTesting;

@CommandLine.Command(
        name = "Neo4j Admin",
        description = "Neo4j Admin CLI."
)
class Neo4jAdminCommand extends BootloaderCommand implements Callable<Integer>
{
    Neo4jAdminCommand( Neo4jAdminBootloaderContext ctx )
    {
        super( ctx );
    }

    static CommandLine asCommandLine( Neo4jAdminBootloaderContext ctx )
    {
        return addDefaultOptions( new CommandLine( new Neo4jAdminCommand( ctx ) ), ctx )
                .setUnmatchedArgumentsAllowed( true )
                .setUnmatchedOptionsArePositionalParams( true );
    }

    @CommandLine.Parameters( hidden = true )
    private List<String> allParameters = List.of();

    @Override
    public Integer call()
    {
        ctx.init( false, false, allParameters.toArray( new String[0] ) );
        Bootloader bootloader = new Bootloader( ctx );
        return bootloader.admin();
    }

    public static void main( String[] args )
    {
        int exitCode = Neo4jAdminCommand.asCommandLine( new Neo4jAdminBootloaderContext() ).execute( args );
        System.exit( exitCode );
    }

    static class Neo4jAdminBootloaderContext extends BootloaderContext
    {
        private static final Class<?> entrypoint = AdminTool.class;

        Neo4jAdminBootloaderContext()
        {
            super( entrypoint );
        }

        @VisibleForTesting
        Neo4jAdminBootloaderContext( PrintStream out, PrintStream err, Function<String,String> envLookup, Function<String,String> propLookup )
        {
            super( out, err, envLookup, propLookup, entrypoint );
        }

        @Override
        Config config()
        {
            return super.config();
        }

        @Override
        protected Map<Setting<?>,Object> overriddenDefaultsValues()
        {
            return Map.of( BootloaderSettings.additional_jvm, "-XX:+UseParallelGC" );
        }
    }
}
