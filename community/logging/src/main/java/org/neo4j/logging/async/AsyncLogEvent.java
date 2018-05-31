/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.logging.async;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;
import org.neo4j.util.concurrent.AsyncEvent;

import static java.util.Objects.requireNonNull;

public final class AsyncLogEvent extends AsyncEvent
{
    static AsyncLogEvent logEvent( @Nonnull Logger logger, @Nonnull String message )
    {
        return new AsyncLogEvent( requireNonNull( logger, "logger" ), requireNonNull( message, "message" ), null );
    }

    static AsyncLogEvent logEvent( @Nonnull Logger logger, @Nonnull String message, @Nonnull Throwable throwable )
    {
        return new AsyncLogEvent( requireNonNull( logger, "logger" ), requireNonNull( message, "message" ),
                requireNonNull( throwable, "Throwable" ) );
    }

    static AsyncLogEvent logEvent( @Nonnull Logger logger, @Nonnull String format, @Nullable Object... arguments )
    {
        return new AsyncLogEvent( requireNonNull( logger, "logger" ), requireNonNull( format, "format" ),
                arguments == null ? new Object[0] : arguments );
    }

    static AsyncLogEvent bulkLogEvent( @Nonnull Log log, @Nonnull final Consumer<Log> consumer )
    {
        requireNonNull( consumer, "Consumer<Log>" );
        return new AsyncLogEvent( requireNonNull( log, "log" ), new BulkLogger()
        {
            @Override
            void process( long timestamp, Object target )
            {
                consumer.accept( (Log) target ); // TODO: include timestamp!
            }

            @Override
            public String toString()
            {
                return "Log.bulkLog( " + consumer + " )";
            }
        } );
    }

    static AsyncLogEvent bulkLogEvent( @Nonnull Logger logger, @Nonnull final Consumer<Logger> consumer )
    {
        requireNonNull( consumer, "Consumer<Logger>" );
        return new AsyncLogEvent( requireNonNull( logger, "logger" ), new BulkLogger()
        {
            @Override
            void process( long timestamp, Object target )
            {
                consumer.accept( (Logger) target ); // TODO: include timestamp!
            }

            @Override
            public String toString()
            {
                return "Logger.bulkLog( " + consumer + " )";
            }
        } );
    }

    public void process()
    {
        if ( parameter == null )
        {
            ((Logger) target).log( "[AsyncLog @ " + timestamp() + "]  " + message );
        }
        else if ( parameter instanceof Throwable )
        {
            ((Logger) target).log( "[AsyncLog @ " + timestamp() + "]  " + message, (Throwable) parameter );
        }
        else if ( parameter instanceof Object[] )
        {
            ((Logger) target).log( "[AsyncLog @ " + timestamp() + "]  " + message, (Object[]) parameter );
        }
        else if ( parameter instanceof BulkLogger )
        {
            ((BulkLogger) parameter).process( timestamp, target );
        }
    }

    private final long timestamp;
    private final Object target;
    private final String message;
    private final Object parameter;

    private AsyncLogEvent( @Nonnull Object target, @Nullable Object parameter )
    {
        this( target, "", parameter );
    }

    private AsyncLogEvent( @Nonnull Object target, @Nonnull String message, @Nullable Object parameter )
    {
        this.target = target;
        this.message = message;
        this.parameter = parameter;
        this.timestamp = System.currentTimeMillis();
    }

    @Override
    public String toString()
    {
        if ( parameter == null )
        {
            return "log( @ " + timestamp() + ": \"" + message + "\" )";
        }
        if ( parameter instanceof Throwable )
        {
            return "log( @ " + timestamp() + ": \"" + message + "\", " + parameter + " )";
        }
        if ( parameter instanceof Object[] )
        {
            return "log( @ " + timestamp() + ": \"" + message + "\", " +
                   Arrays.toString( (Object[]) parameter ) + " )";
        }
        if ( parameter instanceof BulkLogger )
        {
            return parameter.toString();
        }
        return super.toString();
    }

    private abstract static class BulkLogger
    {
        abstract void process( long timestamp, Object target );
    }

    private String timestamp()
    {
        return DATE_FORMAT_THREAD_LOCAL.get().format( new Date( timestamp ) );
    }

    private static final ThreadLocal<DateFormat> DATE_FORMAT_THREAD_LOCAL = ThreadLocal.withInitial( () -> {
        SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSSZ" );
        format.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
        return format;
    } );
}

