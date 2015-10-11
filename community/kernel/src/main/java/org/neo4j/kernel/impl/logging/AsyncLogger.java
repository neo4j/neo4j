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

import org.neo4j.concurrent.AsyncEventSender;
import org.neo4j.function.Consumer;
import org.neo4j.logging.Logger;

import static org.neo4j.kernel.impl.logging.AsyncLogEvent.logEvent;

/**
 * Merely a means of capturing logging statements, converting them to {@link AsyncLogEvent log events}, which are
 * fed into an {@link AsyncEventSender}. These events will be processed later where call to the actual logger
 * will happen.
 */
abstract class AsyncLogger implements Logger
{
    private final AsyncEventSender<AsyncLogEvent> sender;

    AsyncLogger( AsyncEventSender<AsyncLogEvent> sender )
    {
        this.sender = sender;
    }

    /**
     * @return {@link Logger} to be used to log a message right now. The reason this is a method instead
     * of a final instance is that {@link Logger loggers} might change from time to the next due to
     * level configuration changes.
     */
    protected abstract Logger logger();

    /**
     * @return whether or not this {@link Logger} (i.e. this level) is enabled right now. If not then
     * we don't even have to fire log events.
     */
    protected abstract boolean isEnabled();

    @Override
    public void log( String message )
    {
        if ( isEnabled() )
        {
            sender.send( logEvent( logger(), message ) );
        }
    }

    @Override
    public void log( String message, Throwable throwable )
    {
        if ( isEnabled() )
        {
            sender.send( logEvent( logger(), message, throwable ) );
        }
    }

    @Override
    public void log( String format, Object... arguments )
    {
        if ( isEnabled() )
        {
            sender.send( logEvent( logger(), format, arguments ) );
        }
    }

    @Override
    public void bulk( Consumer<Logger> consumer )
    {
        // There should be no need to impose some additional synchronization or flushing here
        // as the method contract hints. TODO perhaps force a flush of the events queue in a finally,
        // although that wouldn't make the actual Logger flush its contents to its target (like I/O output vent).
        consumer.accept( this );
    }
}
