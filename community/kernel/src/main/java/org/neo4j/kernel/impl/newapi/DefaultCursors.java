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
package org.neo4j.kernel.impl.newapi;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.kernel.api.AutoCloseablePlus;

import static java.lang.String.format;

abstract class DefaultCursors
{
    private final Collection<CloseableStacktrace> closeables;
    private final boolean trackCursorClose;
    private final boolean traceCursorClose;

    DefaultCursors( Collection<CloseableStacktrace> closeables, Config config )
    {
        this.closeables = closeables;
        this.trackCursorClose = config.get( GraphDatabaseInternalSettings.track_cursor_close );
        this.traceCursorClose = config.get( GraphDatabaseInternalSettings.trace_cursors );
    }

    protected <T extends AutoCloseablePlus> T trace( T closeable )
    {
        if ( trackCursorClose )
        {
            StackTraceElement[] stackTrace = null;
            if ( traceCursorClose )
            {
                stackTrace = Thread.currentThread().getStackTrace();
                stackTrace = Arrays.copyOfRange( stackTrace, 2, stackTrace.length );
            }

            closeables.add( new CloseableStacktrace( closeable, stackTrace ) );
        }
        return closeable;
    }

    void assertClosed()
    {
        if ( trackCursorClose )
        {
            for ( CloseableStacktrace c : closeables )
            {
                c.assertClosed();
            }
            closeables.clear();
        }
    }

    static class CloseableStacktrace
    {
        private final AutoCloseablePlus c;
        private final StackTraceElement[] stackTrace;

        CloseableStacktrace( AutoCloseablePlus c, StackTraceElement[] stackTrace )
        {
            this.c = c;
            this.stackTrace = stackTrace;
        }

        void assertClosed()
        {
            if ( !c.isClosed() )
            {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                PrintStream printStream = new PrintStream( out, false, StandardCharsets.UTF_8 );

                if ( stackTrace != null )
                {
                    printStream.println();
                    for ( StackTraceElement traceElement : stackTrace )
                    {
                        printStream.println( "\tat " + traceElement );
                    }
                }
                else
                {
                    String msg = format( " To see stack traces please set '%s' setting to true", GraphDatabaseInternalSettings.trace_cursors.name() );
                    printStream.print( msg );
                }
                printStream.println();
                throw new IllegalStateException( format( "Closeable %s was not closed!%s", c, out.toString( StandardCharsets.UTF_8 ) ) );
            }
        }
    }
}
