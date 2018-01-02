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

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Matcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import org.neo4j.concurrent.AsyncEventSender;
import org.neo4j.function.Consumer;
import org.neo4j.logging.AbstractLog;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static java.util.Objects.requireNonNull;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.neo4j.logging.AssertableLogProvider.inLog;

@RunWith( Parameterized.class )
public class AsyncLogTest
{
    @Parameterized.Parameters( name = "{0} {1}.log({2})" )
    public static Iterable<Object[]> parameters()
    {
        List<Object[]> parameters = new ArrayList<>();
        for ( Invocation invocation : Invocation.values() )
        {
            for ( Level level : Level.values() )
            {
                for ( Style style : Style.values() )
                {
                    parameters.add( new Object[]{invocation, level, style} );
                }
            }
        }
        return parameters;
    }

    @SuppressWarnings( "ThrowableInstanceNeverThrown" )
    private final Throwable exception = new Exception();
    private final Invocation invocation;
    private final Level level;
    private final Style style;

    public AsyncLogTest( Invocation invocation, Level level, Style style )
    {
        this.invocation = invocation;
        this.level = level;
        this.style = style;
    }

    @Test
    public void shouldLogAsynchronously() throws Exception
    {
        // given
        AssertableLogProvider logging = new AssertableLogProvider();
        Log log = logging.getLog( getClass() );
        DeferredSender events = new DeferredSender();
        AsyncLog asyncLog = new AsyncLog( events, log );

        // when
        log( invocation.decorate( asyncLog ) );
        // then
        logging.assertNoLoggingOccurred();

        // when
        events.process();
        // then
        MatcherBuilder matcherBuilder = new MatcherBuilder( inLog( getClass() ) );
        log( matcherBuilder );
        logging.assertExactly( matcherBuilder.matcher() );
    }

    private void log( Log log )
    {
        style.invoke( this, level.logger( log ) );
    }

    enum Invocation
    {
        DIRECT
                {
                    @Override
                    Log decorate( Log log )
                    {
                        return new DirectLog( log );
                    }
                },
        INDIRECT
                {
                    @Override
                    Log decorate( Log log )
                    {
                        return log;
                    }
                };

        abstract Log decorate( Log log );
    }

    enum Level
    {
        DEBUG
                {
                    @Override
                    Logger logger( Log log )
                    {
                        return log.debugLogger();
                    }
                },
        INFO
                {
                    @Override
                    Logger logger( Log log )
                    {
                        return log.infoLogger();
                    }
                },
        WARN
                {
                    @Override
                    Logger logger( Log log )
                    {
                        return log.warnLogger();
                    }
                },
        ERROR
                {
                    @Override
                    Logger logger( Log log )
                    {
                        return log.errorLogger();
                    }
                };

        abstract Logger logger( Log log );
    }

    enum Style
    {
        MESSAGE
                {
                    @Override
                    void invoke( AsyncLogTest state, Logger logger )
                    {
                        logger.log( "a message" );
                    }

                    @Override
                    public String toString()
                    {
                        return " <message> ";
                    }
                },
        THROWABLE
                {
                    @Override
                    void invoke( AsyncLogTest state, Logger logger )
                    {
                        logger.log( "an exception", state.exception );
                    }

                    @Override
                    public String toString()
                    {
                        return " <message>, <exception> ";
                    }
                },
        FORMAT
                {
                    @Override
                    void invoke( AsyncLogTest state, Logger logger )
                    {
                        logger.log( "a %s message", "formatted" );
                    }

                    @Override
                    public String toString()
                    {
                        return " <format>, <parameters...> ";
                    }
                };

        abstract void invoke( AsyncLogTest state, Logger logger );
    }

    static class DeferredSender implements AsyncEventSender<AsyncLogEvent>
    {
        private final List<AsyncLogEvent> events = new ArrayList<>();

        @Override
        public void send( AsyncLogEvent event )
        {
            events.add( event );
        }

        public void process()
        {
            for ( AsyncLogEvent event : events )
            {
                event.process();
            }
            events.clear();
        }
    }

    static class DirectLog extends AbstractLog
    {
        final Log log;

        DirectLog( Log log )
        {
            this.log = log;
        }

        @Override
        public boolean isDebugEnabled()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void bulk( Consumer<Log> consumer )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Logger debugLogger()
        {
            return new Logger()
            {
                @Override
                public void log( String message )
                {
                    log.debug( message );
                }

                @Override
                public void log( String message, Throwable throwable )
                {
                    log.debug( message, throwable );
                }

                @Override
                public void log( String format, Object... arguments )
                {
                    log.debug( format, arguments );
                }

                @Override
                public void bulk( Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public Logger infoLogger()
        {
            return new Logger()
            {
                @Override
                public void log( String message )
                {
                    log.info( message );
                }

                @Override
                public void log( String message, Throwable throwable )
                {
                    log.info( message, throwable );
                }

                @Override
                public void log( String format, Object... arguments )
                {
                    log.info( format, arguments );
                }

                @Override
                public void bulk( Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public Logger warnLogger()
        {
            return new Logger()
            {
                @Override
                public void log( String message )
                {
                    log.warn( message );
                }

                @Override
                public void log( String message, Throwable throwable )
                {
                    log.warn( message, throwable );
                }

                @Override
                public void log( String format, Object... arguments )
                {
                    log.warn( format, arguments );
                }

                @Override
                public void bulk( Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public Logger errorLogger()
        {
            return new Logger()
            {
                @Override
                public void log( String message )
                {
                    log.error( message );
                }

                @Override
                public void log( String message, Throwable throwable )
                {
                    log.error( message, throwable );
                }

                @Override
                public void log( String format, Object... arguments )
                {
                    log.error( format, arguments );
                }

                @Override
                public void bulk( Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }
    }

    static class MatcherBuilder extends AbstractLog
    {
        private final AssertableLogProvider.LogMatcherBuilder builder;
        private AssertableLogProvider.LogMatcher matcher;

        public MatcherBuilder( AssertableLogProvider.LogMatcherBuilder builder )
        {
            this.builder = builder;
        }

        public AssertableLogProvider.LogMatcher matcher()
        {
            return requireNonNull( matcher, "invalid use, no matcher built" );
        }

        @Override
        public boolean isDebugEnabled()
        {
            return true;
        }

        @Override
        public void bulk( Consumer<Log> consumer )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Logger debugLogger()
        {
            return new Logger()
            {
                @Override
                public void log( String message )
                {
                    matcher = builder.debug( messageMatcher( message ) );
                }

                @Override
                public void log( String message, Throwable throwable )
                {
                    matcher = builder.debug( messageMatcher( message ), sameInstance( throwable ) );
                }

                @Override
                public void log( String format, Object... arguments )
                {
                    matcher = builder.debug( messageMatcher( format ), arguments );
                }

                @Override
                public void bulk( Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public Logger infoLogger()
        {
            return new Logger()
            {
                @Override
                public void log( String message )
                {
                    matcher = builder.info( messageMatcher( message ) );
                }

                @Override
                public void log( String message, Throwable throwable )
                {
                    matcher = builder.info( messageMatcher( message ), sameInstance( throwable ) );
                }

                @Override
                public void log( String format, Object... arguments )
                {
                    matcher = builder.info( messageMatcher( format ), arguments );
                }

                @Override
                public void bulk( Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public Logger warnLogger()
        {
            return new Logger()
            {
                @Override
                public void log( String message )
                {
                    matcher = builder.warn( messageMatcher( message ) );
                }

                @Override
                public void log( String message, Throwable throwable )
                {
                    matcher = builder.warn( messageMatcher( message ), sameInstance( throwable ) );
                }

                @Override
                public void log( String format, Object... arguments )
                {
                    matcher = builder.warn( messageMatcher( format ), arguments );
                }

                @Override
                public void bulk( Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public Logger errorLogger()
        {
            return new Logger()
            {
                @Override
                public void log( String message )
                {
                    matcher = builder.error( messageMatcher( message ) );
                }

                @Override
                public void log( String message, Throwable throwable )
                {
                    matcher = builder.error( messageMatcher( message ), sameInstance( throwable ) );
                }

                @Override
                public void log( String format, Object... arguments )
                {
                    matcher = builder.error( messageMatcher( format ), arguments );
                }

                @Override
                public void bulk( Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        private Matcher<String> messageMatcher( String message )
        {
            return allOf( startsWith( "[AsyncLog @ " ), endsWith( "]  " + message ) );
        }
    }
}