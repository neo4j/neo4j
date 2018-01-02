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
package org.neo4j.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.LinkedList;
import java.util.Queue;

import org.neo4j.function.Consumer;

/**
 * Buffers all messages sent to it, and is able to replay those messages into
 * another Logger.
 *
 * This can be used to start up services that need logging when they start, but
 * where, for one reason or another, we have not yet set up proper logging in
 * the application lifecycle.
 *
 * This will replay messages in the order they are received, *however*, it will
 * not preserve the time stamps of the original messages.
 *
 * You should not use this for logging messages where the time stamps are
 * important.
 *
 * You should also not use this logger, when there is a risk that it can be
 * subjected to an unbounded quantity of log messages, since the buffer keeps
 * all messages until it gets a chance to replay them.
 */
public class BufferingLog extends AbstractLog
{
    private interface LogMessage
    {
        void replayInto( Log other );

        void printTo( PrintWriter pw );
    }

    private final Queue<LogMessage> buffer = new LinkedList<>();

    private abstract class BufferingLogger implements Logger
    {
        @Override
        public void log( String message )
        {
            LogMessage logMessage = buildMessage( message );
            synchronized (buffer)
            {
                buffer.add( logMessage );
            }
        }

        protected abstract LogMessage buildMessage( String message );

        @Override
        public void log( final String message, final Throwable throwable )
        {
            LogMessage logMessage = buildMessage( message, throwable );
            synchronized (buffer)
            {
                buffer.add( logMessage );
            }
        }

        protected abstract LogMessage buildMessage( String message, Throwable throwable );

        @Override
        public void log( String format, Object... arguments )
        {
            LogMessage logMessage = buildMessage( format, arguments );
            synchronized (buffer)
            {
                buffer.add( logMessage );
            }
        }

        protected abstract LogMessage buildMessage( String message, Object... arguments );

        @Override
        public void bulk( Consumer<Logger> consumer )
        {
            synchronized (buffer)
            {
                consumer.accept( this );
            }
        }
    }

    private final Logger debugLogger = new BufferingLogger()
    {
        @Override
        public LogMessage buildMessage( final String message )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.debug( message );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( message );
                }
            };
        }

        @Override
        public LogMessage buildMessage( final String message, final Throwable throwable )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.debug( message, throwable );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( message );
                    throwable.printStackTrace( pw );
                }
            };
        }

        @Override
        public LogMessage buildMessage( final String format, final Object... arguments )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.debug( format, arguments );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( String.format( format, arguments ) );
                }
            };
        }
    };

    private final Logger infoLogger = new BufferingLogger()
    {
        @Override
        public LogMessage buildMessage( final String message )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.info( message );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( message );
                }
            };
        }

        @Override
        public LogMessage buildMessage( final String message, final Throwable throwable )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.info( message, throwable );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( message );
                    throwable.printStackTrace( pw );
                }
            };
        }

        @Override
        public LogMessage buildMessage( final String format, final Object... arguments )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.info( format, arguments );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( String.format( format, arguments ) );
                }
            };
        }
    };

    private final Logger warnLogger = new BufferingLogger()
    {
        @Override
        public LogMessage buildMessage( final String message )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.warn( message );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( message );
                }
            };
        }

        @Override
        public LogMessage buildMessage( final String message, final Throwable throwable )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.warn( message, throwable );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( message );
                    throwable.printStackTrace( pw );
                }
            };
        }

        @Override
        public LogMessage buildMessage( final String format, final Object... arguments )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.warn( format, arguments );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( String.format( format, arguments ) );
                }
            };
        }
    };

    private final Logger errorLogger = new BufferingLogger()
    {
        @Override
        public LogMessage buildMessage( final String message )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.error( message );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( message );
                }
            };
        }

        @Override
        public LogMessage buildMessage( final String message, final Throwable throwable )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.error( message, throwable );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( message );
                    throwable.printStackTrace( pw );
                }
            };
        }

        @Override
        public LogMessage buildMessage( final String format, final Object... arguments )
        {
            return new LogMessage()
            {
                @Override
                public void replayInto( Log other )
                {
                    other.error( format, arguments );
                }

                @Override
                public void printTo( PrintWriter pw )
                {
                    pw.println( String.format( format, arguments ) );
                }
            };
        }
    };


    @Override
    public boolean isDebugEnabled()
    {
        return true;
    }

    @Override
    public Logger debugLogger()
    {
        return this.debugLogger;
    }

    @Override
    public Logger infoLogger()
    {
        return infoLogger;
    }

    @Override
    public Logger warnLogger()
    {
        return warnLogger;
    }

    @Override
    public Logger errorLogger()
    {
        return errorLogger;
    }

    @Override
    public void bulk( Consumer<Log> consumer )
    {
        synchronized (buffer)
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
        synchronized (buffer)
        {
            LogMessage message = buffer.poll();
            while ( message != null )
            {
                message.replayInto( other );
                message = buffer.poll();
            }
        }
    }

    @Override
    public String toString()
    {
        synchronized (buffer)
        {
            StringWriter stringWriter = new StringWriter();
            PrintWriter sb = new PrintWriter( stringWriter );
            for ( LogMessage message : buffer )
            {
                message.printTo( sb );
            }
            return stringWriter.toString();
        }
    }
}
