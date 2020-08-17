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
package org.neo4j.logging.log4j;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import org.neo4j.logging.Level;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.logging.log4j.LogConfigTest.DATE_PATTERN;

class Log4jLogTest
{
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    private Log4jLog log;
    private Neo4jLoggerContext context;

    @BeforeEach
    void setUp()
    {
        context = LogConfig.createBuilder( outContent, Level.DEBUG ).build();
        log = new Log4jLog( context.getLogger( "className" ) );
    }

    @AfterEach
    void tearDown()
    {
        context.close();
    }

    @ParameterizedTest( name = "{1}" )
    @MethodSource( "logMethods" )
    void shouldWriteMessage( LogMethod logMethod, Level level )
    {
        logMethod.log( log, "my message" );

        assertThat( outContent.toString() ).matches( format( DATE_PATTERN + " %-5s \\[className\\] my message%n", level ) );
    }

    @ParameterizedTest( name = "{1}" )
    @MethodSource( "logMethods" )
    void shouldWriteMessageAndThrowable( LogMethod logMethod, Level level )
    {
        Throwable throwable = newThrowable( "stacktrace" );
        logMethod.log( log, "my message", throwable );
        String throwableName = throwable.getClass().getName();

        assertThat( outContent.toString() ).matches( format( DATE_PATTERN + " %-5s \\[className\\] my message%n" + Pattern.quote( throwableName ) +
                                                             ": stacktrace%n", level ) );
    }

    @ParameterizedTest( name = "{1}" )
    @MethodSource( "logMethods" )
    void shouldWriteMessageWithFormat( LogMethod logMethod, Level level )
    {
        logMethod.log( log, "my %s message %d", "long", 1 );

        assertThat( outContent.toString() ).matches( format( DATE_PATTERN + " %-5s \\[className\\] my long message 1%n", level ) );
    }

    private interface LogMethod
    {
        void log( Log4jLog logger, String msg );

        void log( Log4jLog logger, String msg, Throwable cause );

        void log( Log4jLog logger, String format, Object... arguments );
    }

    private static Stream<Arguments> logMethods()
    {
        LogMethod debug = new LogMethod()
        {
            public void log( Log4jLog logger, String msg )
            {
                logger.debug( msg );
            }

            public void log( Log4jLog logger, String msg, Throwable cause )
            {
                logger.debug( msg, cause );
            }

            public void log( Log4jLog logger, String format, Object... arguments )
            {
                logger.debug( format, arguments );
            }
        };
        LogMethod info = new LogMethod()
        {
            public void log( Log4jLog logger, String msg )
            {
                logger.info( msg );
            }

            public void log( Log4jLog logger, String msg, Throwable cause )
            {
                logger.info( msg, cause );
            }

            public void log( Log4jLog logger, String format, Object... arguments )
            {
                logger.info( format, arguments );
            }
        };
        LogMethod warn = new LogMethod()
        {
            public void log( Log4jLog logger, String msg )
            {
                logger.warn( msg );
            }

            public void log( Log4jLog logger, String msg, Throwable cause )
            {
                logger.warn( msg, cause );
            }

            public void log( Log4jLog logger, String format, Object... arguments )
            {
                logger.warn( format, arguments );
            }
        };
        LogMethod error = new LogMethod()
        {
            public void log( Log4jLog logger, String msg )
            {
                logger.error( msg );
            }

            public void log( Log4jLog logger, String msg, Throwable cause )
            {
                logger.error( msg, cause );
            }

            public void log( Log4jLog logger, String format, Object... arguments )
            {
                logger.error( format, arguments );
            }
        };

        return Stream.of(
                Arguments.of( debug, Level.DEBUG ),
                Arguments.of( info, Level.INFO ),
                Arguments.of( warn, Level.WARN ),
                Arguments.of( error, Level.ERROR ) );
    }

    private static Throwable newThrowable( final String message )
    {
        return new Throwable()
        {
            @Override
            public StackTraceElement[] getStackTrace()
            {
                return new StackTraceElement[]{};
            }

            @Override
            public String getMessage()
            {
                return message;
            }
        };
    }
}
