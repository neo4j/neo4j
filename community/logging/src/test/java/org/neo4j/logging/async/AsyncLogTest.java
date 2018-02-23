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

import org.hamcrest.Matcher;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.neo4j.concurrent.AsyncEventSender;
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

public class AsyncLogTest
{
    public static Iterable<Arguments> parameters()
    {
        List<Arguments> parameters = new ArrayList<>();
        for ( Invocation invocation : Invocation.values() )
        {
            for ( Level level : Level.values() )
            {
                for ( Style style : Style.values() )
                {
                    parameters.add( Arguments.of( invocation, level, style ) );
                }
            }
        }
        return parameters;
    }

    @SuppressWarnings( "ThrowableInstanceNeverThrown" )
    private final Throwable exception = new Exception();

    @ParameterizedTest( name = "{0} {1}.log({2})" )
    @MethodSource( "parameters" )
    public void shouldLogAsynchronously( Invocation invocation, Level level, Style style )
    {
        // given
        AssertableLogProvider logging = new AssertableLogProvider();
        Log log = logging.getLog( getClass() );
        DeferredSender events = new DeferredSender();
        AsyncLog asyncLog = new AsyncLog( events, log );

        // when
        log( invocation.decorate( asyncLog ), level, style );
        // then
        logging.assertNoLoggingOccurred();

        // when
        events.process();
        // then
        MatcherBuilder matcherBuilder = new MatcherBuilder( inLog( getClass() ) );
        log( matcherBuilder, level, style );
        logging.assertExactly( matcherBuilder.matcher() );
    }

    private void log( Log log, Level level, Style style )
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
        public void bulk( @Nonnull Consumer<Log> consumer )
        {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public Logger debugLogger()
        {
            return new Logger()
            {
                @Override
                public void log( @Nonnull String message )
                {
                    log.debug( message );
                }

                @Override
                public void log( @Nonnull String message, @Nonnull Throwable throwable )
                {
                    log.debug( message, throwable );
                }

                @Override
                public void log( @Nonnull String format, @Nonnull Object... arguments )
                {
                    log.debug( format, arguments );
                }

                @Override
                public void bulk( @Nonnull Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Nonnull
        @Override
        public Logger infoLogger()
        {
            return new Logger()
            {
                @Override
                public void log( @Nonnull String message )
                {
                    log.info( message );
                }

                @Override
                public void log( @Nonnull String message, @Nonnull Throwable throwable )
                {
                    log.info( message, throwable );
                }

                @Override
                public void log( @Nonnull String format, @Nonnull Object... arguments )
                {
                    log.info( format, arguments );
                }

                @Override
                public void bulk( @Nonnull Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Nonnull
        @Override
        public Logger warnLogger()
        {
            return new Logger()
            {
                @Override
                public void log( @Nonnull String message )
                {
                    log.warn( message );
                }

                @Override
                public void log( @Nonnull String message, @Nonnull Throwable throwable )
                {
                    log.warn( message, throwable );
                }

                @Override
                public void log( @Nonnull String format, @Nonnull Object... arguments )
                {
                    log.warn( format, arguments );
                }

                @Override
                public void bulk( @Nonnull Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Nonnull
        @Override
        public Logger errorLogger()
        {
            return new Logger()
            {
                @Override
                public void log( @Nonnull String message )
                {
                    log.error( message );
                }

                @Override
                public void log( @Nonnull String message, @Nonnull Throwable throwable )
                {
                    log.error( message, throwable );
                }

                @Override
                public void log( @Nonnull String format, @Nonnull Object... arguments )
                {
                    log.error( format, arguments );
                }

                @Override
                public void bulk( @Nonnull Consumer<Logger> consumer )
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

        MatcherBuilder( AssertableLogProvider.LogMatcherBuilder builder )
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
        public void bulk( @Nonnull Consumer<Log> consumer )
        {
            throw new UnsupportedOperationException();
        }

        @Nonnull
        @Override
        public Logger debugLogger()
        {
            return new Logger()
            {
                @Override
                public void log( @Nonnull String message )
                {
                    matcher = builder.debug( messageMatcher( message ) );
                }

                @Override
                public void log( @Nonnull String message, @Nonnull Throwable throwable )
                {
                    matcher = builder.debug( messageMatcher( message ), sameInstance( throwable ) );
                }

                @Override
                public void log( @Nonnull String format, @Nonnull Object... arguments )
                {
                    matcher = builder.debug( messageMatcher( format ), arguments );
                }

                @Override
                public void bulk( @Nonnull Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Nonnull
        @Override
        public Logger infoLogger()
        {
            return new Logger()
            {
                @Override
                public void log( @Nonnull String message )
                {
                    matcher = builder.info( messageMatcher( message ) );
                }

                @Override
                public void log( @Nonnull String message, @Nonnull Throwable throwable )
                {
                    matcher = builder.info( messageMatcher( message ), sameInstance( throwable ) );
                }

                @Override
                public void log( @Nonnull String format, @Nonnull Object... arguments )
                {
                    matcher = builder.info( messageMatcher( format ), arguments );
                }

                @Override
                public void bulk( @Nonnull Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Nonnull
        @Override
        public Logger warnLogger()
        {
            return new Logger()
            {
                @Override
                public void log( @Nonnull String message )
                {
                    matcher = builder.warn( messageMatcher( message ) );
                }

                @Override
                public void log( @Nonnull String message, @Nonnull Throwable throwable )
                {
                    matcher = builder.warn( messageMatcher( message ), sameInstance( throwable ) );
                }

                @Override
                public void log( @Nonnull String format, @Nonnull Object... arguments )
                {
                    matcher = builder.warn( messageMatcher( format ), arguments );
                }

                @Override
                public void bulk( @Nonnull Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Nonnull
        @Override
        public Logger errorLogger()
        {
            return new Logger()
            {
                @Override
                public void log( @Nonnull String message )
                {
                    matcher = builder.error( messageMatcher( message ) );
                }

                @Override
                public void log( @Nonnull String message, @Nonnull Throwable throwable )
                {
                    matcher = builder.error( messageMatcher( message ), sameInstance( throwable ) );
                }

                @Override
                public void log( @Nonnull String format, @Nonnull Object... arguments )
                {
                    matcher = builder.error( messageMatcher( format ), arguments );
                }

                @Override
                public void bulk( @Nonnull Consumer<Logger> consumer )
                {
                    throw new UnsupportedOperationException();
                }
            };
        }

        private Matcher<String> messageMatcher( @Nonnull String message )
        {
            return allOf( startsWith( "[AsyncLog @ " ), endsWith( "]  " + message ) );
        }
    }
}
