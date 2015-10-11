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
package org.neo4j.kernel.impl.logging;

import java.util.concurrent.Executor;

import org.neo4j.concurrent.AsyncEventSender;
import org.neo4j.concurrent.AsyncEvents;
import org.neo4j.function.Consumer;
import org.neo4j.kernel.impl.util.CappedOperation;
import org.neo4j.kernel.impl.util.Listener;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.kernel.impl.util.CappedOperation.differentItemClasses;
import static org.neo4j.kernel.impl.util.CappedOperation.time;

class AsyncEventLogging extends LifecycleAdapter implements Consumer<AsyncLogEvent>, AsyncEventSender<AsyncLogEvent>
{
    private final AsyncEvents<AsyncLogEvent> events;
    private final CappedOperation<Throwable> errorNotification = new CappedOperation<Throwable>(
            differentItemClasses(), time( 1, SECONDS ) )
    {
        @Override
        protected void triggered( Throwable error )
        {
            errorHandler.receive( error );
        }
    };
    private final Listener<Throwable> errorHandler;
    private final Executor executor;
    static final Listener<Throwable> DEFAULT_ASYNC_ERROR_HANDLER = new Listener<Throwable>()
    {
        @Override
        public void receive( Throwable notification )
        {
            notification.printStackTrace();
        }
    };

    AsyncEventLogging( Listener<Throwable> errorHandler, Executor executor )
    {
        this.errorHandler = errorHandler;
        this.executor = executor;
        this.events = new AsyncEvents<>( this );
    }

    @Override
    public void accept( AsyncLogEvent logEvent )
    {
        try
        {
            logEvent.process();
        }
        catch ( Throwable t )
        {
            // We catch Throwable here otherwise there's a high chance an error, like OutOfMemory
            // or similar will halt the logging from that point and onwards, since all logging
            // is processed by a dedicated thread.
            //
            // What's most important is that logging gets going, but we also can be nice and...
            // well... not log, since apparently there was a problem doing that right now, but do something.
            // That something will be to print stack trace of the exception, capped by different
            // exception types and time interval for good measure.
            try
            {
                errorNotification.event( t );
            }
            catch ( Throwable e )
            {
                // Even that failed. There's no reasonable thing left to do here, is there?
            }
        }
    }

    @Override
    public void init() throws Throwable
    {
        executor.execute( events );
    }

    @Override
    public void shutdown() throws Throwable
    {
        events.shutdown();
        events.awaitTermination();
    }

    @Override
    public void send( AsyncLogEvent event )
    {
        events.send( event );
    }
}
