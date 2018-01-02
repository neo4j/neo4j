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
package org.neo4j.logging.async;

import org.neo4j.concurrent.AsyncEventSender;
import org.neo4j.function.Consumer;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.util.Objects.requireNonNull;
import static org.neo4j.logging.async.AsyncLogEvent.bulkLogEvent;
import static org.neo4j.logging.async.AsyncLogEvent.logEvent;

public class AsyncLog extends AbstractLog
{
    private final Log log;
    private final AsyncEventSender<AsyncLogEvent> events;

    public AsyncLog( AsyncEventSender<AsyncLogEvent> events, Log log )
    {
        this.log = requireNonNull( log, "Log" );
        this.events = requireNonNull( events, "AsyncEventSender<AsyncLogEvent>" );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return log.isDebugEnabled();
    }

    @Override
    public Logger debugLogger()
    {
        return new AsyncLogger( events, log.debugLogger() );
    }

    @Override
    public Logger infoLogger()
    {
        return new AsyncLogger( events, log.infoLogger() );
    }

    @Override
    public Logger warnLogger()
    {
        return new AsyncLogger( events, log.warnLogger() );
    }

    @Override
    public Logger errorLogger()
    {
        return new AsyncLogger( events, log.errorLogger() );
    }

    @Override
    public void bulk( Consumer<Log> consumer )
    {
        events.send( bulkLogEvent( log, consumer ) );
    }

    private static class AsyncLogger implements Logger
    {
        private final Logger logger;
        private final AsyncEventSender<AsyncLogEvent> events;

        AsyncLogger( AsyncEventSender<AsyncLogEvent> events, Logger logger )
        {
            this.logger = requireNonNull( logger, "Logger" );
            this.events = events;
        }

        @Override
        public void log( String message )
        {
            events.send( logEvent( logger, message ) );
        }

        @Override
        public void log( String message, Throwable throwable )
        {
            events.send( logEvent( logger, message, throwable ) );
        }

        @Override
        public void log( String format, Object... arguments )
        {
            events.send( logEvent( logger, format, arguments ) );
        }

        @Override
        public void bulk( Consumer<Logger> consumer )
        {
            events.send( bulkLogEvent( logger, consumer ) );
        }
    }
}
