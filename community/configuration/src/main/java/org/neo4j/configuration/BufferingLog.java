/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.configuration;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.Log;

/**
 * Buffers all messages sent to it, and is able to replay those messages into
 * another Logger.
 * <p>
 * This can be used to start up services that need logging when they start, but
 * where, for one reason or another, we have not yet set up proper logging in
 * the application lifecycle.
 * <p>
 * This will replay messages in the order they are received, *however*, it will
 * not preserve the time stamps of the original messages.
 * <p>
 * You should not use this for logging messages where the time stamps are
 * important.
 * <p>
 * You should also not use this logger, when there is a risk that it can be
 * subjected to an unbounded quantity of log messages, since the buffer keeps
 * all messages until it gets a chance to replay them.
 */
public class BufferingLog extends AbstractLog
{
    @FunctionalInterface
    private interface LogMessage
    {
        void replayInto( Log other );
    }

    private final Queue<LogMessage> buffer = new ArrayDeque<>();

    @Override
    public boolean isDebugEnabled()
    {
        return true;
    }

    @Override
    public void debug( @Nonnull String message )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.debug( message ) );
        }
    }

    @Override
    public void debug( @Nonnull String message, @Nonnull Throwable throwable )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.debug( message, throwable ) );
        }
    }

    @Override
    public void debug( @Nonnull String format, @Nullable Object... arguments )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.debug( format, arguments ) );
        }
    }

    @Override
    public void info( @Nonnull String message )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.info( message ) );
        }
    }

    @Override
    public void info( @Nonnull String message, @Nonnull Throwable throwable )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.info( message, throwable ) );
        }
    }

    @Override
    public void info( @Nonnull String format, @Nullable Object... arguments )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.info( format, arguments ) );
        }
    }

    @Override
    public void warn( @Nonnull String message )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.warn( message ) );
        }
    }

    @Override
    public void warn( @Nonnull String message, @Nonnull Throwable throwable )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.warn( message, throwable ) );
        }
    }

    @Override
    public void warn( @Nonnull String format, @Nullable Object... arguments )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.warn( format, arguments ) );
        }
    }

    @Override
    public void error( @Nonnull String message )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.error( message ) );
        }
    }

    @Override
    public void error( @Nonnull String message, @Nonnull Throwable throwable )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.error( message, throwable ) );
        }
    }

    @Override
    public void error( @Nonnull String format, @Nullable Object... arguments )
    {
        synchronized ( buffer )
        {
            buffer.add( other -> other.error( format, arguments ) );
        }
    }

    @Override
    public void bulk( @Nonnull Consumer<Log> consumer )
    {
        synchronized ( buffer )
        {
            consumer.accept( this );
        }
    }

    /**
     * Replays buffered messages and clears the buffer.
     *
     * @param other the log to reply into
     */
    public void replayInto( Log other )
    {
        synchronized ( buffer )
        {
            LogMessage message = buffer.poll();
            while ( message != null )
            {
                message.replayInto( other );
                message = buffer.poll();
            }
        }
    }
}
