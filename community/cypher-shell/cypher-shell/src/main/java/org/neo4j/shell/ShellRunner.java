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
package org.neo4j.shell;

import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.io.output.WriterOutputStream;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import javax.annotation.Nonnull;

import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.FileHistorian;
import org.neo4j.shell.cli.InteractiveShellRunner;
import org.neo4j.shell.cli.NonInteractiveShellRunner;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.ShellStatementParser;

import static org.fusesource.jansi.internal.CLibrary.STDIN_FILENO;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;
import static org.neo4j.shell.system.Utils.isWindows;

public interface ShellRunner
{

    /**
     * Get an appropriate shellrunner depending on the given arguments and if we are running in a TTY.
     *
     * @param cliArgs
     * @param cypherShell
     * @param logger
     * @param connectionConfig
     * @return a ShellRunner
     * @throws IOException
     */
    @Nonnull
    static ShellRunner getShellRunner( @Nonnull CliArgs cliArgs,
                                       @Nonnull CypherShell cypherShell,
                                       @Nonnull Logger logger,
                                       @Nonnull ConnectionConfig connectionConfig ) throws IOException
    {
        if ( shouldBeInteractive( cliArgs ) )
        {
            UserMessagesHandler userMessagesHandler =
                    new UserMessagesHandler( connectionConfig, cypherShell.getServerVersion() );
            return new InteractiveShellRunner( cypherShell, cypherShell, cypherShell, logger, new ShellStatementParser(),
                                               System.in, FileHistorian.getDefaultHistoryFile(), userMessagesHandler, connectionConfig );
        }
        else
        {

            return new NonInteractiveShellRunner( cliArgs.getFailBehavior(), cypherShell, logger,
                                                  new ShellStatementParser(), getInputStream( cliArgs ) );
        }
    }

    /**
     * @param cliArgs
     * @return true if an interactive shellrunner should be used, false otherwise
     */
    static boolean shouldBeInteractive( @Nonnull CliArgs cliArgs )
    {
        if ( cliArgs.getNonInteractive() || cliArgs.getInputFilename() != null )
        {
            return false;
        }

        return isInputInteractive();
    }

    /**
     * Checks if STDIN is a TTY. In case TTY checking is not possible (lack of libc), then the check falls back to the built in Java {@link System#console()}
     * which checks if EITHER STDIN or STDOUT has been redirected.
     *
     * @return true if the shell is reading from an interactive terminal, false otherwise (e.g., we are reading from a file).
     */
    static boolean isInputInteractive()
    {
        if ( isWindows() )
        {
            // Input will never be a TTY on windows and it isatty seems to be able to block forever on Windows so avoid
            // calling it.
            return System.console() != null;
        }
        try
        {
            return 1 == isatty( STDIN_FILENO );
        }
        catch ( Throwable ignored )
        {
            // system is not using libc (like Alpine Linux)
            // Fallback to checking stdin OR stdout
            return System.console() != null;
        }
    }

    /**
     * Checks if STDOUT is a TTY. In case TTY checking is not possible (lack of libc), then the check falls back to the built in Java {@link System#console()}
     * which checks if EITHER STDIN or STDOUT has been redirected.
     *
     * @return true if the shell is outputting to an interactive terminal, false otherwise (e.g., we are outputting to a file)
     */
    static boolean isOutputInteractive()
    {
        if ( isWindows() )
        {
            // Input will never be a TTY on windows and it isatty seems to be able to block forever on Windows so avoid
            // calling it.
            return System.console() != null;
        }
        try
        {
            return 1 == isatty( STDOUT_FILENO );
        }
        catch ( Throwable ignored )
        {
            // system is not using libc (like Alpine Linux)
            // Fallback to checking stdin OR stdout
            return System.console() != null;
        }
    }

    /**
     * If an input file has been defined use that, otherwise use STDIN
     *
     * @throws FileNotFoundException if the provided input file doesn't exist
     */
    static InputStream getInputStream( CliArgs cliArgs ) throws FileNotFoundException
    {
        if ( cliArgs.getInputFilename() == null )
        {
            return System.in;
        }
        else
        {
            return new BufferedInputStream( new FileInputStream( new File( cliArgs.getInputFilename() ) ) );
        }
    }

    static OutputStream getOutputStreamForInteractivePrompt()
    {
        if ( isWindows() )
        {
            // Output will never be a TTY on windows and it isatty seems to be able to block forever on Windows so avoid
            // calling it.
            if ( System.console() != null )
            {
                return new WriterOutputStream( System.console().writer(), Charset.defaultCharset() );
            }
        }
        else
        {
            try
            {
                if ( 1 == isatty( STDOUT_FILENO ) )
                {
                    return System.out;
                }
                else
                {
                    return new FileOutputStream( new File( "/dev/tty" ) );
                }
            }
            catch ( Throwable ignored )
            {
                // system is not using libc (like Alpine Linux)
                // Fallback to checking stdin OR stdout
                if ( System.console() != null )
                {
                    return new WriterOutputStream( System.console().writer(), Charset.defaultCharset() );
                }
            }
        }
        return new NullOutputStream();
    }

    /**
     * Run and handle user input until end of file
     *
     * @return error code to exit with
     */
    int runUntilEnd();

    /**
     * @return an object which can provide the history of commands executed
     */
    @Nonnull
    Historian getHistorian();
}
