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
package org.neo4j.test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * A simple Runnable that is meant to consume the output and error streams of a
 * detached process, for debugging purposes.
 */
public class StreamConsumer implements Runnable
{
    public interface StreamExceptionHandler
    {
        void handle( IOException failure );
    }
    
    public static StreamExceptionHandler PRINT_FAILURES = new StreamExceptionHandler()
    {
        @Override
        public void handle( IOException failure )
        {
            failure.printStackTrace();
        }
    };
    
    public static StreamExceptionHandler IGNORE_FAILURES = new StreamExceptionHandler()
    {
        @Override
        public void handle( IOException failure )
        {
        }
    };
    
    private final BufferedReader in;
    private final Writer out;

    private final boolean quiet;
    private final String prefix;

    private final StreamExceptionHandler failureHandler;
    private final Exception stackTraceOfOrigin;

    public StreamConsumer( InputStream in, OutputStream out, boolean quiet )
    {
        this( in, out, quiet, "", quiet ? IGNORE_FAILURES : PRINT_FAILURES );
    }

    public StreamConsumer( InputStream in, OutputStream out, boolean quiet, String prefix,
            StreamExceptionHandler failureHandler )
    {
        this.quiet = quiet;
        this.prefix = prefix;
        this.failureHandler = failureHandler;
        this.in = new BufferedReader(new InputStreamReader( in ));
        this.out = new OutputStreamWriter( out );
        this.stackTraceOfOrigin = new Exception("Stack trace of thread that created this StreamConsumer");
    }

    @Override
    public void run()
    {
        try
        {
            String line;
            while ( ( line = in.readLine()) != null)
            {
                if ( !quiet )
                {
                    out.write( prefix+line+"\n" );
                    out.flush();
                }
            }
        }
        catch ( IOException exc )
        {
            exc.addSuppressed( stackTraceOfOrigin );
            failureHandler.handle( exc );
        }
    }
}