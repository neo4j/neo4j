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
package org.neo4j.logging;

import java.io.PrintStream;
import java.util.Arrays;
import java.util.IllegalFormatException;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nonnull;

import static java.lang.String.format;
import static org.apache.commons.text.StringEscapeUtils.escapeJava;

public class AssertableLogProvider extends AbstractLogProvider<Log>
{
    private final boolean debugEnabled;
    private final Queue<LogCall> logCalls = new LinkedBlockingQueue<>();

    public AssertableLogProvider()
    {
        this( false );
    }

    public AssertableLogProvider( boolean debugEnabled )
    {
        this.debugEnabled = debugEnabled;
    }

    public void print( PrintStream out )
    {
        for ( LogCall call : logCalls )
        {
            out.println( call.toLogLikeString() );
            if ( call.throwable != null )
            {
                call.throwable.printStackTrace( out );
            }
        }
    }

    Queue<LogCall> getLogCalls()
    {
        return logCalls;
    }

    public enum Level
    {
        DEBUG,
        INFO,
        WARN,
        ERROR
    }

    static final class LogCall
    {
        private final String context;
        private final Level level;
        private final String message;
        private final Object[] arguments;
        private final Throwable throwable;

        private LogCall( String context, Level level, String message, Object[] arguments, Throwable throwable )
        {
            this.level = level;
            this.context = context;
            this.message = message;
            this.arguments = arguments;
            this.throwable = throwable;
        }

        String getContext()
        {
            return context;
        }

        Level getLevel()
        {
            return level;
        }

        String getMessage()
        {
            return message;
        }

        Object[] getArguments()
        {
            return arguments;
        }

        Throwable getThrowable()
        {
            return throwable;
        }

        @Override
        public String toString()
        {
            StringBuilder builder = new StringBuilder( "LogCall{" );
            builder.append( context );
            builder.append( ' ' );
            builder.append( level );
            builder.append( ", message=" );
            if ( message != null )
            {
                builder.append( '\'' ).append( escapeJava( message ) ).append( '\'' );
            }
            else
            {
                builder.append( "null" );
            }
            builder.append( ", arguments=" );
            if ( arguments != null )
            {
                builder.append( '[' );
                boolean first = true;
                for ( Object arg : arguments )
                {
                    if ( !first )
                    {
                        builder.append( ',' );
                    }
                    first = false;
                    builder.append( escapeJava( String.valueOf( arg ) ) );
                }
                builder.append( ']' );
            }
            else
            {
                builder.append( "null" );
            }
            builder.append( ", throwable=" );
            if ( throwable != null )
            {
                builder.append( '\'' ).append( escapeJava( throwable.toString() ) ).append( '\'' );
            }
            else
            {
                builder.append( "null" );
            }
            builder.append( '}' );
            return builder.toString();
        }

        String toLogLikeString()
        {
            String msg;
            if ( arguments != null )
            {
                try
                {
                    msg = format( message, arguments );
                }
                catch ( IllegalFormatException e )
                {
                    msg = format( "IllegalFormat{message: \"%s\", arguments: %s}",
                            message, Arrays.toString( arguments ) );
                }
            }
            else
            {
                msg = message;
            }
            return format( "%s @ %s: %s", level, context, msg );
        }
    }

    private class LogCallRecorder implements Logger
    {
        private final String context;
        private final Level level;

        LogCallRecorder( String context, Level level )
        {
            this.context = context;
            this.level = level;
        }

        @Override
        public void log( @Nonnull String message )
        {
            logCalls.add( new LogCall( context, level, message, null, null ) );
        }

        @Override
        public void log( @Nonnull String message, @Nonnull Throwable throwable )
        {
            logCalls.add( new LogCall( context, level, message, null, throwable ) );
        }

        @Override
        public void log( @Nonnull String format, @Nonnull Object... arguments )
        {
            logCalls.add( new LogCall( context, level, format, arguments, null ) );
        }

        @Override
        public void bulk( @Nonnull Consumer<Logger> consumer )
        {
            consumer.accept( this );
        }
    }

    protected class AssertableLog extends AbstractLog
    {
        private final Logger debugLogger;
        private final Logger infoLogger;
        private final Logger warnLogger;
        private final Logger errorLogger;

        AssertableLog( String context )
        {
            this.debugLogger = new LogCallRecorder( context, Level.DEBUG );
            this.infoLogger = new LogCallRecorder( context, Level.INFO );
            this.warnLogger = new LogCallRecorder( context, Level.WARN );
            this.errorLogger = new LogCallRecorder( context, Level.ERROR );
        }

        @Override
        public boolean isDebugEnabled()
        {
            return debugEnabled;
        }

        @Nonnull
        @Override
        public Logger debugLogger()
        {
            return debugLogger;
        }

        @Nonnull
        @Override
        public Logger infoLogger()
        {
            return infoLogger;
        }

        @Nonnull
        @Override
        public Logger warnLogger()
        {
            return warnLogger;
        }

        @Nonnull
        @Override
        public Logger errorLogger()
        {
            return errorLogger;
        }

        @Override
        public void bulk( @Nonnull Consumer<Log> consumer )
        {
            consumer.accept( this );
        }
    }

    @Override
    protected Log buildLog( Class<?> loggingClass )
    {
        return new AssertableLog( loggingClass.getName() );
    }

    @Override
    protected Log buildLog( String context )
    {
        return new AssertableLog( context );
    }

    /**
     * Clear this logger for re-use.
     */
    public void clear()
    {
        logCalls.clear();
    }

    public String serialize()
    {
        return serialize0( logCalls.iterator(), LogCall::toLogLikeString );
    }

    private static String serialize0( Iterator<LogCall> events, Function<LogCall,String> serializer )
    {
        StringBuilder sb = new StringBuilder();
        while ( events.hasNext() )
        {
            sb.append( serializer.apply( events.next() ) );
            sb.append( '\n' );
        }
        return sb.toString();
    }
}
