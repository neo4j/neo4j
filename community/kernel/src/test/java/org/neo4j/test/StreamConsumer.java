/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
 *
 */
public class StreamConsumer implements Runnable
{

    private final BufferedReader in;
    private final Writer out;

    private boolean quiet;
    private String prefix;

    public StreamConsumer( InputStream in, OutputStream out, boolean quiet )
    {
        this(in, out, quiet, "");
    }

    public StreamConsumer( InputStream in, OutputStream out, boolean quiet, String prefix )
    {
        this.quiet = quiet;
        this.prefix = prefix;
        this.in = new BufferedReader(new InputStreamReader( in ));
        this.out = new OutputStreamWriter( out );
    }

    @Override
    public void run()
    {
        try
        {
            String line;
            while ( ( line = in.readLine()) != null)
            {
                if (!quiet)
                {
                    out.write( prefix+line+"\n" );
                    out.flush();
                }
            }
        }
        catch ( IOException exc )
        {
            System.err.println( "Child I/O Transfer - " + exc );
        }
    }
}