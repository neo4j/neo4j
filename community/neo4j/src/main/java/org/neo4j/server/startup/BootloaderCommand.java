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
import picocli.CommandLine.Option;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

import static org.neo4j.server.startup.Bootloader.ARG_EXPAND_COMMANDS;

@CommandLine.Command( sortOptions = false )
abstract class BootloaderCommand
{
    protected final BootloaderContext ctx;

    BootloaderCommand( BootloaderContext ctx )
    {
        this.ctx = ctx;
    }

    protected abstract static class BaseCommand implements Callable<Integer>, VerboseCommand
    {
        @CommandLine.ParentCommand
        protected BootloaderCommand bootloader;

        @Option( names = ARG_VERBOSE, description = "Prints additional information." )
        boolean verbose;

        Bootloader getBootloader( boolean expandCommands )
        {
            bootloader.ctx.init( expandCommands, verbose );
            return new Bootloader( bootloader.ctx );
        }

        @Override
        public boolean verbose()
        {
            return verbose;
        }
    }

    protected abstract static class BootCommand extends BaseCommand
    {
        @CommandLine.Mixin
        protected StartOptions startOptions;

        static class StartOptions
        {
            @Option( names = ARG_EXPAND_COMMANDS, description = "Allow command expansion in config value evaluation." )
            boolean expandCommands;
        }
    }

    protected static CommandLine addDefaultOptions( CommandLine command, BootloaderContext ctx )
    {
        return command.setCaseInsensitiveEnumValuesAllowed( true )
                .setExecutionExceptionHandler( new ExceptionHandler( ctx ) )
                .setOut( new PrintWriter( ctx.out, true ) )
                .setErr( new PrintWriter( ctx.err, true ) );
    }

    private static class ExceptionHandler implements CommandLine.IExecutionExceptionHandler
    {
        private final BootloaderContext ctx;
        ExceptionHandler( BootloaderContext ctx )
        {
            this.ctx = ctx;
        }

        @Override
        public int handleExecutionException( Exception exception, CommandLine commandLine, CommandLine.ParseResult parseResult )
        {
            if ( commandLine.getCommand() instanceof VerboseCommand && !((VerboseCommand) commandLine.getCommand()).verbose() )
            {
                ctx.err.println( exception.getMessage() );
                ctx.err.println( "Run with '--verbose' for a more detailed error message.");
            }
            else
            {
                exception.printStackTrace( ctx.err );
            }
            if ( exception instanceof BootFailureException failure )
            {
                return failure.getExitCode();
            }
            return commandLine.getCommandSpec().exitCodeOnExecutionException();
        }
    }
}
