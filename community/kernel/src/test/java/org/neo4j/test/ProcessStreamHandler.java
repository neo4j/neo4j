/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Having trouble with your {@link Process}'s output and error streams?
 * Are they getting filled up and your main thread hangs? Fear no more. Use this
 * class to bundle everything up in one nifty little object that handles all
 * your needs. Use the {@code ProcessStreamHandler#launch()} method to start
 * the consumer threads and once you are {@code ProcessStreamHandler#done()}
 * just say so.
 */
public class ProcessStreamHandler
{
    private final Thread out;
    private final Thread err;

    /**
     * Convenience constructor assuming the local output streams are
     * {@link System.out} and {@link System.err} for the process's OutputStream
     * and ErrorStream respectively.
     *
     * @param toHandle The process whose output to consume.
     */
    public ProcessStreamHandler( Process toHandle )
    {
        this( toHandle.getInputStream(), toHandle.getErrorStream(), System.out,
                System.err );
    }

    /**
     * Fine grained constructor for redirecting the input streams to the output
     * streams provided.
     *
     * @param processOutput The redirected output stream
     * @param processError The redirected error stream
     * @param ourOutput The end output stream
     * @param ourError The end error stream
     */
    public ProcessStreamHandler( InputStream processOutput,
            InputStream processError, OutputStream ourOutput,
            OutputStream ourError )
    {
        out = new Thread( new StreamConsumer( processOutput, ourOutput ) );
        err = new Thread( new StreamConsumer( processError, ourError ) );
    }

    /**
     * Starts the consumer threads. Calls {@link Thread#start()}.
     */
    public void launch()
    {
        out.start();
        err.start();
    }

    /**
     * Joins with the consumer Threads. Calls {@link Thread#join()} on the two
     * consumers.
     *
     * @throws InterruptedException
     */
    public void done() throws InterruptedException
    {
        out.join();
        err.join();
    }
}
