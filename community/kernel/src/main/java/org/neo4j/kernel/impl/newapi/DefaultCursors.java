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
package org.neo4j.kernel.impl.newapi;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;

import org.neo4j.internal.kernel.api.AutoCloseablePlus;

import static java.lang.String.format;
import static org.neo4j.util.FeatureToggles.flag;

abstract class DefaultCursors
{
    private static final boolean DEBUG_CLOSING = flag( DefaultCursors.class, "trackCursors", false );
    private final Collection<CloseableStacktrace> closeables;

    DefaultCursors( Collection<CloseableStacktrace> closeables )
    {
        this.closeables = closeables;
    }

    protected <T extends AutoCloseablePlus> T trace( T closeable )
    {
        if ( DEBUG_CLOSING )
        {
            StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();
            closeables.add( new CloseableStacktrace( closeable, Arrays.copyOfRange( stackTrace, 2, stackTrace.length ) ) );
        }
        return closeable;
    }

    void assertClosed()
    {
        if ( DEBUG_CLOSING )
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
                PrintStream printStream = new PrintStream( out );

                for ( StackTraceElement traceElement : stackTrace )
                {
                    printStream.println( "\tat " + traceElement );
                }
                printStream.println();
                throw new IllegalStateException( format( "Closeable %s was not closed!\n%s", c, out.toString() ) );
            }
        }
    }

}
