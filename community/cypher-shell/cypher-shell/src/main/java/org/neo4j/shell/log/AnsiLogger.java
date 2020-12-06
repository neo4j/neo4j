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
package org.neo4j.shell.log;

import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.exception.AnsiFormattedException;

import static org.fusesource.jansi.internal.CLibrary.STDERR_FILENO;
import static org.fusesource.jansi.internal.CLibrary.STDOUT_FILENO;
import static org.fusesource.jansi.internal.CLibrary.isatty;

/**
 * A basic logger which prints Ansi formatted text to STDOUT and STDERR
 */
public class AnsiLogger implements Logger
{
    private final PrintStream out;
    private final PrintStream err;
    private final boolean debug;
    private Format format;

    public AnsiLogger( final boolean debug )
    {
        this( debug, Format.VERBOSE, System.out, System.err );
    }

    public AnsiLogger( final boolean debug, @Nonnull Format format,
                       @Nonnull PrintStream out, @Nonnull PrintStream err )
    {
        this.debug = debug;
        this.format = format;
        this.out = out;
        this.err = err;

        try
        {
            if ( isOutputInteractive() )
            {
                Ansi.setEnabled( true );
                AnsiConsole.systemInstall();
            }
            else
            {
                Ansi.setEnabled( false );
            }
        }
        catch ( Throwable t )
        {
            // Not running on a distro with standard c library, disable Ansi.
            Ansi.setEnabled( false );
        }
    }

    @Nonnull
    private static Throwable getRootCause( @Nonnull final Throwable th )
    {
        Throwable cause = th;
        while ( cause.getCause() != null )
        {
            cause = cause.getCause();
        }
        return cause;
    }

    /**
     * @return true if the shell is outputting to a TTY, false otherwise (e.g., we are writing to a file)
     * @throws UnsatisfiedLinkError maybe if standard c library can't be found
     * @throws NoClassDefFoundError maybe if standard c library can't be found
     */
    private static boolean isOutputInteractive()
    {
        return 1 == isatty( STDOUT_FILENO ) && 1 == isatty( STDERR_FILENO );
    }

    @Nonnull
    @Override
    public PrintStream getOutputStream()
    {
        return out;
    }

    @Nonnull
    @Override
    public PrintStream getErrorStream()
    {
        return err;
    }

    @Nonnull
    @Override
    public Format getFormat()
    {
        return format;
    }

    @Override
    public void setFormat( @Nonnull Format format )
    {
        this.format = format;
    }

    @Override
    public boolean isDebugEnabled()
    {
        return debug;
    }

    @Override
    public void printError( @Nonnull Throwable throwable )
    {
        printError( getFormattedMessage( throwable ) );
    }

    @Override
    public void printError( @Nonnull String s )
    {
        err.println( Ansi.ansi().render( s ).toString() );
    }

    @Override
    public void printOut( @Nonnull final String msg )
    {
        out.println( Ansi.ansi().render( msg ).toString() );
    }

    /**
     * Formatting for Bolt exceptions.
     */
    @Nonnull
    public String getFormattedMessage( @Nonnull final Throwable e )
    {
        AnsiFormattedText msg = AnsiFormattedText.s().colorRed();

        if ( isDebugEnabled() )
        {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            PrintStream ps = new PrintStream( baos );
            e.printStackTrace( ps );
            msg.append( new String( baos.toByteArray(), StandardCharsets.UTF_8 ) );
        }
        else
        {
            if ( e instanceof AnsiFormattedException )
            {
                msg = msg.append( ((AnsiFormattedException) e).getFormattedMessage() );
            }
            else if ( e instanceof ClientException &&
                      e.getMessage() != null && e.getMessage().contains( "Missing username" ) )
            {
                // Username and password was not specified
                msg = msg.append( e.getMessage() )
                         .append( "\nPlease specify --username, and optionally --password, as argument(s)" )
                         .append( "\nor as environment variable(s), NEO4J_USERNAME, and NEO4J_PASSWORD respectively." )
                         .append( "\nSee --help for more info." );
            }
            else
            {
                Throwable cause = e;

                // Get the suppressed root cause of ServiceUnavailableExceptions
                if ( e instanceof ServiceUnavailableException )
                {
                    Throwable[] suppressed = e.getSuppressed();
                    for ( Throwable s : suppressed )
                    {
                        if ( s instanceof DiscoveryException )
                        {
                            cause = getRootCause( s );
                            break;
                        }
                    }
                }

                if ( cause.getMessage() != null )
                {
                    msg = msg.append( cause.getMessage() );
                }
                else
                {
                    msg = msg.append( cause.getClass().getSimpleName() );
                }
            }
        }

        return msg.formattedString();
    }
}
