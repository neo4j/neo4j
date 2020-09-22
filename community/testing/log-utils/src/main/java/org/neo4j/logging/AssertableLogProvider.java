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
import javax.annotation.Nullable;

import org.neo4j.logging.log4j.Neo4jLogMessage;
import org.neo4j.logging.log4j.StructureAwareMessage;

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
            if ( throwable != null )
            {
                msg = msg + " cause " + throwable.toString();
            }
            return format( "%s @ %s: %s", level, context, msg );
        }
    }

    protected class AssertableLog extends AbstractLog
    {
        private final String context;

        AssertableLog( String context )
        {
            this.context = context;
        }

        @Override
        public boolean isDebugEnabled()
        {
            return debugEnabled;
        }

        @Override
        public void debug( @Nonnull String message )
        {
            logCalls.add( new LogCall( context, Level.DEBUG, message, null, null ) );
        }

        @Override
        public void debug( @Nonnull String message, @Nonnull Throwable throwable )
        {
            logCalls.add( new LogCall( context, Level.DEBUG, message, null, throwable ) );
        }

        @Override
        public void debug( @Nonnull String format, @Nullable Object... arguments )
        {
            logCalls.add( new LogCall( context, Level.DEBUG, format, arguments, null ) );
        }

        @Override
        public void info( @Nonnull String message )
        {
            logCalls.add( new LogCall( context, Level.INFO, message, null, null ) );
        }

        @Override
        public void info( @Nonnull String message, @Nonnull Throwable throwable )
        {
            logCalls.add( new LogCall( context, Level.INFO, message, null, throwable ) );
        }

        @Override
        public void info( @Nonnull String format, @Nullable Object... arguments )
        {
            logCalls.add( new LogCall( context, Level.INFO, format, arguments, null ) );
        }

        @Override
        public void warn( @Nonnull String message )
        {
            logCalls.add( new LogCall( context, Level.WARN, message, null, null ) );
        }

        @Override
        public void warn( @Nonnull String message, @Nonnull Throwable throwable )
        {
            logCalls.add( new LogCall( context, Level.WARN, message, null, throwable ) );
        }

        @Override
        public void warn( @Nonnull String format, @Nullable Object... arguments )
        {
            logCalls.add( new LogCall( context, Level.WARN, format, arguments, null ) );
        }

        @Override
        public void error( @Nonnull String message )
        {
            logCalls.add( new LogCall( context, Level.ERROR, message, null, null ) );
        }

        @Override
        public void error( @Nonnull String message, @Nonnull Throwable throwable )
        {
            logCalls.add( new LogCall( context, Level.ERROR, message, null, throwable ) );
        }

        @Override
        public void error( @Nonnull String format, @Nullable Object... arguments )
        {
            logCalls.add( new LogCall( context, Level.ERROR, format, arguments, null ) );
        }

        @Override
        public void info( Neo4jLogMessage message )
        {
            if ( message instanceof StructureAwareMessage )
            {
                StructureAwareMessage msg = (StructureAwareMessage) message;
                logCalls.add( new LogCall( context, Level.INFO, msg.getFormattedMessage(), null, null ) );
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void error( Neo4jLogMessage message )
        {
            if ( message instanceof StructureAwareMessage )
            {
                StructureAwareMessage msg = (StructureAwareMessage) message;
                logCalls.add( new LogCall( context, Level.ERROR, msg.getFormattedMessage(), null, null ) );
            }
            else
            {
                throw new UnsupportedOperationException();
            }
        }

        @Override
        public void error( Neo4jLogMessage message, Throwable throwable )
        {
            if ( message instanceof StructureAwareMessage )
            {
                StructureAwareMessage msg = (StructureAwareMessage) message;
                logCalls.add( new LogCall( context, Level.ERROR, msg.getFormattedMessage(), null, throwable ) );
            }
            else
            {
                throw new UnsupportedOperationException();
            }
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
