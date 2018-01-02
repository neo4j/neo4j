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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.TimeZone;

import org.neo4j.concurrent.AsyncEvent;
import org.neo4j.function.Consumer;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.util.Objects.requireNonNull;

public final class AsyncLogEvent extends AsyncEvent
{
    static AsyncLogEvent logEvent( Logger logger, String message )
    {
        return new AsyncLogEvent( logger, requireNonNull( message, "message" ), null );
    }

    static AsyncLogEvent logEvent( Logger logger, String message, Throwable throwable )
    {
        return new AsyncLogEvent( logger, requireNonNull( message, "message" ),
                requireNonNull( throwable, "Throwable" ) );
    }

    static AsyncLogEvent logEvent( Logger logger, String format, Object... arguments )
    {
        return new AsyncLogEvent( logger, requireNonNull( format, "format" ),
                arguments == null ? new Object[0] : arguments );
    }

    static AsyncLogEvent bulkLogEvent( Log log, final Consumer<Log> consumer )
    {
        requireNonNull( consumer, "Consumer<Log>" );
        return new AsyncLogEvent( log, null, new BulkLogger()
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

    static AsyncLogEvent bulkLogEvent( Logger logger, final Consumer<Logger> consumer )
    {
        requireNonNull( consumer, "Consumer<Logger>" );
        return new AsyncLogEvent( logger, null, new BulkLogger()
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

    @SuppressWarnings( "StringEquality" )
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

    private AsyncLogEvent( Object target, String message, Object parameter )
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

    private String timestamp()
    {
        return TIMESTAMP.format( timestamp );
    }

    private static abstract class BulkLogger
    {
        abstract void process( long timestamp, Object target );
    }

    private static final ThreadLocalFormat TIMESTAMP = new ThreadLocalFormat();

    private static class ThreadLocalFormat extends ThreadLocal<DateFormat>
    {
        String format( long timestamp )
        {
            return get().format( new Date( timestamp ) );
        }

        @Override
        protected DateFormat initialValue()
        {
            SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss.SSSZ" );
            format.setTimeZone( TimeZone.getTimeZone( "UTC" ) );
            return format;
        }
    }
}

