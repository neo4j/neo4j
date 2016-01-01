/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.*;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.debug;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.error;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.info;
import static org.neo4j.kernel.impl.util.TestLogger.LogCall.warn;

/**
 * This is so meta.
 */
public class TestTestLogger
{
    @Test
    public void shouldPassExactAssertions() throws Exception
    {
        // Given
        TestLogger logger = new TestLogger();

        Throwable cause = new Throwable( "This is a throwable!" );

        logger.debug( "Debug" );
        logger.debug( "Debug", cause );
        logger.info( "Info" );
        logger.info( "Info", cause );
        logger.warn( "Warn" );
        logger.warn( "Warn", cause );
        logger.error( "Error" );
        logger.error( "Error", cause );

        // When
        logger.assertExactly(
                debug( "Debug" ),
                debug( "Debug", cause ),
                info( "Info" ),
                info( "Info", cause ),
                warn( "Warn" ),
                warn( "Warn", cause ),
                error( "Error" ),
                error( "Error", cause )
        );

        // Then no assertion should have failed
    }

    @Test
    public void shouldFailNicelyIfTooManyLogCalls() throws Exception
    {
        // Given
        TestLogger logger = new TestLogger();
        logger.debug( "Debug" );
        logger.debug( "Debug 2" );

        // When
        try
        {
            logger.assertExactly( debug( "Debug" ) );
            fail( "Should have failed." );
        }
        catch ( AssertionError e )
        {
            // Then
            assertThat( e.getMessage(), equalTo(
                    "Got more log calls than expected. The remaining log calls were: \n" +
                    "LogCall{level=DEBUG, message='Debug 2', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
            ) );
        }
    }

    @Test
    public void shouldFailIfTooFewLogCalls() throws Exception
    {
        // Given
        TestLogger logger = new TestLogger();

        // When
        try
        {
            logger.assertExactly( debug( "Debug" ) );
            fail( "Should have failed." );
        }
        catch ( AssertionError e )
        {
            // Then
            assertThat( e.getMessage(), equalTo(
                    "Got fewer log calls than expected. The missing log calls were: \n" +
                    "LogCall{level=DEBUG, message='Debug', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
            ) );
        }
    }

    @Test
    public void shouldPassIfContains() throws Exception
    {
        // Given
        TestLogger logger = new TestLogger();

        logger.debug( "Debug 1" );
        logger.debug( "Debug 2" );
        logger.debug( "Debug 3" );

        // When
        logger.assertAtLeastOnce(
                debug( "Debug 2" ),
                debug( "Debug 1" ) );

        // Then no assertion should have failed
    }

    @Test
    public void shouldFailIfDoesntContain() throws Exception
    {
        // Given
        TestLogger logger = new TestLogger();

        logger.debug( "Debug 1" );
        logger.debug( "Debug 3" );

        // When
        try
        {
            logger.assertAtLeastOnce(
                    debug( "Debug 2" ),
                    debug( "Debug 1" ) );
        }
        catch ( AssertionError e )
        {
            assertThat( e.getMessage(), equalTo(
                    "These log calls were expected, but never occurred: \n"
                    +
                    "LogCall{level=DEBUG, message='Debug 2', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n\n"
                    +
                    "Actual log calls were:\n"
                    +
                    "LogCall{level=DEBUG, message='Debug 1', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
                    +
                    "LogCall{level=DEBUG, message='Debug 3', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
            ) );
        }
    }

    @Test
    public void shouldPassOnEmptyAsserters() throws Exception
    {
        // Given
        TestLogger logger = new TestLogger();

        // When
        logger.assertNoDebugs();
        logger.assertNoInfos();
        logger.assertNoWarnings();
        logger.assertNoErrors();
        logger.assertNoLoggingOccurred();

        // Then no assertions should have failed
    }

    @Test
    public void shouldFailOnNonEmptyAsserters() throws Exception
    {
        TestLogger logger = new TestLogger();

        // Debug
        try
        {
            logger.debug( "Debug" );
            logger.assertNoErrors();
            logger.assertNoWarnings();
            logger.assertNoInfos();
            logger.assertNoDebugs();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            assertThat( e.getMessage(), equalTo(
                    "Expected no messages with level DEBUG. But found: \n" +
                    "LogCall{level=DEBUG, message='Debug', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
            ) );
        }

        // Info
        try
        {
            logger.info( "Info" );
            logger.assertNoErrors();
            logger.assertNoWarnings();
            logger.assertNoInfos();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            assertThat( e.getMessage(), equalTo(
                    "Expected no messages with level INFO. But found: \n" +
                    "LogCall{level=INFO, message='Info', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n" )
            );
        }

        // Warn
        try
        {
            logger.warn( "Warn" );
            logger.assertNoErrors();
            logger.assertNoWarnings();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            assertThat( e.getMessage(), equalTo(
                    "Expected no messages with level WARN. But found: \n" +
                    "LogCall{level=WARN, message='Warn', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n" )
            );
        }

        // Errors
        try
        {
            logger.error( "Error" );
            logger.assertNoErrors();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            assertThat( e.getMessage(), equalTo(
                    "Expected no messages with level ERROR. But found: \n" +
                    "LogCall{level=ERROR, message='Error', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
            ) );
        }

        // All
        try
        {
            logger.assertNoLoggingOccurred();
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            assertThat( e.getMessage(), equalTo(
                    "Expected no log messages at all, but got:\n"
                    +
                    "LogCall{level=DEBUG, message='Debug', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
                    +
                    "LogCall{level=INFO, message='Info', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
                    +
                    "LogCall{level=WARN, message='Warn', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
                    +
                    "LogCall{level=ERROR, message='Error', cause=null, flush=false, marker=LogMarker{name='no_mark'}}\n"
            ) );
        }

        // Specific message
        try
        {
            logger.debug( "This is a message." );
            logger.debug( "This is a message." );
            logger.assertNo( debug( "This is a message." ) );
            fail( "Should have failed" );
        }
        catch ( AssertionError e )
        {
            assertThat( e.getMessage(), equalTo(
                    "Expected no occurrence of " +
                    "LogCall{level=DEBUG, message='This is a message.', cause=null, flush=false, marker=LogMarker{name='no_mark'}}, " +
                    "but it was in fact logged 2 times." ) );
        }
    }

}
