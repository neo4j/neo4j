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
package org.neo4j.shell.log;

import java.io.PrintStream;

import org.neo4j.shell.cli.Format;
import org.neo4j.shell.prettyprint.LinePrinter;

public interface Logger extends LinePrinter
{
    /**
     * @return the output stream
     */
    PrintStream getOutputStream();

    /**
     * @return the error stream
     */
    PrintStream getErrorStream();

    /**
     * Print a sanitized cause of the specified error. If debug mode is enabled, a full stacktrace should be printed as well.
     *
     * @param throwable to print to the error stream
     */
    void printError( Throwable throwable );

    /**
     * Print the designated text to configured error stream.
     *
     * @param text to print to the error stream
     */
    void printError( String text );

    /**
     * @return the current format of the logger
     */
    Format getFormat();

    /**
     * Set the output format on the logger
     *
     * @param format to set
     */
    void setFormat( Format format );

    /**
     * @return true if debug mode is enabled, false otherwise
     */
    boolean isDebugEnabled();

    /**
     * Convenience method which only prints the given text to the output stream if debug mode is enabled
     *
     * @param text to print to the output stream
     */
    default void printIfDebug( String text )
    {
        if ( isDebugEnabled() )
        {
            printOut( text );
        }
    }

    /**
     * Convenience method which only prints the given text to the output stream if the format set is {@link Format#VERBOSE}.
     *
     * @param text to print to the output stream
     */
    default void printIfVerbose( String text )
    {
        if ( Format.VERBOSE.equals( getFormat() ) )
        {
            printOut( text );
        }
    }

    /**
     * Convenience method which only prints the given text to the output stream if the format set is {@link Format#PLAIN}.
     *
     * @param text to print to the output stream
     */
    default void printIfPlain( String text )
    {
        if ( Format.PLAIN.equals( getFormat() ) )
        {
            printOut( text );
        }
    }
}
