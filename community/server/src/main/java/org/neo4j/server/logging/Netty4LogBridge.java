/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.logging;

import io.netty.util.internal.logging.InternalLoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;

import org.neo4j.logging.LogProvider;

/**
 * Glue code for swapping in Neo4j logging into Netty4
 */
public class Netty4LogBridge
{
    private static final PrintStream NULL_OUTPUT = new PrintStream( new NullOutput() );

    public static void setLogProvider( LogProvider logProvider )
    {
        // TODO: Undo below hack in next minor/major release
        // Netty 4 will look for and use Slf4j if it's on the classpath. In this release (2.3.x),
        // it is on the classpath, via `logback-classic`. However, we do not configure logback,
        // meaning some debug output leaks to stdout before we replace the logging below.
        // This should be fixed properly in the next release that is not a patch release.
        PrintStream originalStdOut = System.out;
        try
        {
            System.setOut( NULL_OUTPUT );
            InternalLoggerFactory.setDefaultFactory( new Netty4LoggerFactory( logProvider ) );
        }
        finally
        {
            System.setOut( originalStdOut );
        }

    }

    private static class NullOutput extends OutputStream
    {
        @Override
        public void write( int b ) throws IOException
        {
            // no-op
        }
    }
}
