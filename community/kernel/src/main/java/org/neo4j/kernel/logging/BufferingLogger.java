/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
    private enum Level
    {
        DEBUG, INFO, WARN, ERROR
    }

    private static class LogMessage
    {
        private final String message;
        private final Throwable throwable;
        private final boolean flush;
        private final LogMarker logMarker;
        private final Level level;

        public LogMessage( String message, Throwable throwable, boolean flush, LogMarker logMarker, Level level )
        {
            this.message = message;
            this.throwable = throwable;
            this.flush = flush;
            this.logMarker = logMarker;
            this.level = level;
        }
    }

    private final Queue<LogMessage> buffer = new ConcurrentLinkedQueue<>();

    private void log( String msg, Throwable cause, boolean flush, LogMarker logMarker, Level level )
    {
        buffer.add( new LogMessage( msg, cause, flush, logMarker, level ) );
    }

    @Override
    protected void doDebug( String msg, Throwable cause, boolean flush, LogMarker logMarker )
    {
        log( msg, cause, flush, logMarker, Level.DEBUG );
    }

    @Override
    public void info( String msg, Throwable cause, boolean flush, LogMarker logMarker )
    {
        log( msg, cause, flush, logMarker, Level.INFO );
    }

    @Override
    public void warn( String msg, Throwable cause, boolean flush, LogMarker logMarker )
    {
        log( msg, cause, flush, logMarker, Level.WARN );
    }

    @Override
    public void error( String msg, Throwable cause, boolean flush, LogMarker logMarker )
    {
        log( msg, cause, flush, logMarker, Level.ERROR );
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
        info( line );
    }

    @Override
    public void flush()
    {
        info( "", true );
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
        for  ( LogMessage message; ( message = buffer.poll()) != null ; )
        {
            switch ( message.level )
            {
            case DEBUG:
                other.debug( message.message, message.throwable, message.flush, message.logMarker );
                break;
            case INFO:
                other.info( message.message, message.throwable, message.flush, message.logMarker );
                break;
            case WARN:
                other.warn( message.message, message.throwable, message.flush, message.logMarker );
                break;
            case ERROR:
                other.error( message.message, message.throwable, message.flush, message.logMarker );
                break;
            default:
                throw new IllegalStateException( "Unknown log level " + message.level );
            }
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
