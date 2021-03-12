/*
 * Copyright (c) "Neo4j"
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
import org.junit.jupiter.params.provider.Arguments;

import java.io.ByteArrayOutputStream;
import java.util.stream.Stream;

import org.neo4j.logging.Level;

abstract class Log4jLogTestBase
{
    protected final ByteArrayOutputStream outContent = new ByteArrayOutputStream();
    protected Log4jLog log;
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

    protected interface LogMethod
    {
        void log( Log4jLog logger, String msg );

        void log( Log4jLog logger, String msg, Throwable cause );

        void log( Log4jLog logger, String format, Object... arguments );

        void log( Log4jLog logger, Neo4jMessageSupplier supplier );
    }

    protected static Stream<Arguments> logMethods()
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

            @Override
            public void log( Log4jLog logger, Neo4jMessageSupplier supplier )
            {
                logger.debug( supplier );
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

            @Override
            public void log( Log4jLog logger, Neo4jMessageSupplier supplier )
            {
                logger.info( supplier );
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

            @Override
            public void log( Log4jLog logger, Neo4jMessageSupplier supplier )
            {
                logger.warn( supplier );
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

            @Override
            public void log( Log4jLog logger, Neo4jMessageSupplier supplier )
            {
                logger.error( supplier );
            }
        };

        return Stream.of(
                Arguments.of( debug, Level.DEBUG ),
                Arguments.of( info, Level.INFO ),
                Arguments.of( warn, Level.WARN ),
                Arguments.of( error, Level.ERROR ) );
    }

    protected static Throwable newThrowable( final String message )
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
