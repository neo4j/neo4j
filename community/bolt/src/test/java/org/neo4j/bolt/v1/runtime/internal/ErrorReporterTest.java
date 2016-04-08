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
package org.neo4j.bolt.v1.runtime.internal;

import org.junit.Test;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.Log;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ErrorReporterTest
{
    @Test
    public void shouldLogFullMessageInDebugLogAndHelpfulPointerInUserLog() throws Exception
    {
        // given
        Log userLog = mock( Log.class );
        Log internalLog = mock( Log.class );
        ErrorReporter reporter = new ErrorReporter( userLog, internalLog );

        Throwable cause = new Throwable( "Hello World" );
        Neo4jError error = Neo4jError.from( cause );

        // when
        reporter.report( error );

        // then
        verify( userLog ).error( "Client triggered an unexpected error: Hello World. See debug.log for more details." );
        verify( internalLog ).error( contains( "java.lang.Throwable: Hello World" ) );
    }

    @Test
    public void shouldReportUnknownErrorsInUserLog()
    {
        // Given
        AssertableLogProvider provider = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( provider.getLog( "userLog" ), provider.getLog( "internalLog" ) );

        Throwable cause = new Throwable( "This is not an error we know how to handle." );
        Neo4jError error = Neo4jError.from( cause );

        // When
        reporter.report( error );

        // Then
        assertThat( error.status(), equalTo( (Status) Status.General.UnknownError ) );
        provider.assertContainsMessageContaining( error.cause().getMessage() );
    }

    @Test
    public void shouldNotReportOOMErrorsInUserLog()
    {
        // Given
        AssertableLogProvider provider = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( provider.getLog( "userLog" ), provider.getLog( "internalLog" ) );

        Throwable cause = new OutOfMemoryError( "memory is fading" );
        Neo4jError error = Neo4jError.from( cause );

        // When
        reporter.report( error );

        // Then
        assertThat( error.status(), equalTo( (Status) Status.General.OutOfMemoryError ) );
        assertThat( error.message(),
                equalTo( "There is not enough memory to perform the current task. Please try increasing " +
                        "'dbms.memory.heap.max_size' in the process wrapper configuration (normally in " +
                        "'conf/neo4j-wrapper" +
                        ".conf' or, if you are using Neo4j Desktop, found through the user interface) or if you are " +
                        "running an embedded " +
                        "installation increase the heap by using '-Xmx' command line flag, and then restart the " +
                        "database." ) );
        provider.assertContainsMessageContaining( cause.getClass().getName() );
    }

    @Test
    public void shouldReportStackOverflowErrorsInInternalLog()
    {
        // Given
        AssertableLogProvider provider = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( provider.getLog( "userLog" ), provider.getLog( "internalLog" ) );

        Throwable cause = new StackOverflowError( "some rewriter is probably not tail recursive" );
        Neo4jError error = Neo4jError.from( cause );

        // When
        reporter.report( error );

        // Then
        assertThat( error.status(), equalTo( (Status) Status.General.StackOverFlowError ) );
        assertThat( error.message(), equalTo(
                "There is not enough stack size to perform the current task. This is generally considered to be a " +
                        "database error, so please contact Neo4j support. You could try increasing the stack size: " +
                        "for example to set the stack size to 2M, add `dbms.jvm.additional=-Xss2M' to " +
                        "in the process wrapper configuration (normally in 'conf/neo4j-wrapper.conf' or, if you are " +
                        "using " +
                        "Neo4j Desktop, found through the user interface) or if you are running an embedded " +
                        "installation " +
                        "just add -Xss2M as command line flag." ) );
        provider.assertContainsMessageContaining( cause.getClass().getName() );
    }
}