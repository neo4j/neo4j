/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Buffers all messages sent to it, and is able to replay those messages into
 * another StringLogger.
 *
 * This can be used to start up services that need logging when they start, but
 * where, for one reason or another, we have not yet set up proper logging in
 * the application lifecycle.
 *
 * This will replay messages in the order they are recieved, *however*, it will
 * not preserve the time stamps of the original messages.
 *
 * You should not use this for logging messages where the time stamps are
 * important.
 *
 * You should also not use this logger, when there is a risk that it can be
 * subjected to an unbounded quantity of log messages, since the buffer keeps
 * all messages until it gets a chance to replay them.
 */
public class BufferingLogger extends StringLogger
{
    private static class LogMessage
    {
        private final String message;
        private final Throwable throwable;
        private final boolean flush;

        public LogMessage( String message, Throwable throwable, boolean flush )
        {
            this.message = message;
            this.throwable = throwable;
            this.flush = flush;
        }
    }

    private final Queue<LogMessage> buffer = new ConcurrentLinkedQueue<>();

    @Override
    public void logMessage( String msg )
    {
        logMessage( msg, null, false );
    }

    @Override
    public void logMessage( String msg, Throwable throwable )
    {
        logMessage( msg, throwable, false );
    }

    @Override
    public void logMessage( String msg, boolean flush )
    {
        logMessage( msg, null, flush );
    }

    @Override
    public void logMessage( String msg, LogMarker marker )
    {
        logMessage( msg );
    }

    @Override
    public void logMessage( String msg, Throwable cause, boolean flush )
    {
        buffer.add( new LogMessage( msg, cause, flush ) );
    }

    @Override
    public void logLongMessage( String msg, Visitor<LineLogger, RuntimeException> source, boolean flush )
    {
        source.visit( new LineLoggerImpl( this ) );
    }

    @Override
    public void addRotationListener( Runnable listener )
    {
    }

    @Override
    protected void logLine( String line )
    {
        logMessage( line );
    }

    @Override
    public void flush()
    {
        logMessage( "", true );
    }

    @Override
    public void close()
    {
        // no-op
    }

    /**
     * Replays buffered messages and clears the buffer.
     */
    public void replayInto( StringLogger other )
    {
        LogMessage message = buffer.poll();
        while ( message != null )
        {
            if ( message.throwable != null )
            {
                other.logMessage( message.message, message.throwable, message.flush );
            }
            else
            {
                other.logMessage( message.message, message.flush );
            }
            message = buffer.poll();
        }
    }

    @Override
    public String toString()
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter sb = new PrintWriter( stringWriter );
        for ( LogMessage message : buffer )
        {
            sb.println( message.message );
            if ( message.throwable != null )
            {
                message.throwable.printStackTrace( sb );
            }
        }
        return stringWriter.toString();
    }
}
