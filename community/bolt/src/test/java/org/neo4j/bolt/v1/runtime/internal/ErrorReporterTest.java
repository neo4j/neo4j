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

import org.hamcrest.CoreMatchers;
import org.junit.Test;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.udc.UsageData;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.logging.AssertableLogProvider.inLog;

public class ErrorReporterTest
{
    @Test
    public void shouldReportUnknownErrors()
    {
        // Given
        AssertableLogProvider provider = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( provider.getLog( "userlog" ), new UsageData( mock( JobScheduler.class ) ) );

        Throwable cause = new Throwable( "This is not an error we know how to handle." );
        Neo4jError error = Neo4jError.from( cause );

        // When
        reporter.report( error );

        // Then
        assertThat( error.status(), equalTo( (Status) Status.General.UnknownError ) );
        provider.assertExactly(
                inLog( "userlog" )
                        .error( CoreMatchers.both( CoreMatchers.containsString( "START OF REPORT" ) )
                                .and( CoreMatchers.containsString( "END OF REPORT" ) ) ) );
        provider.assertExactly(
                inLog( "userlog" ).error( CoreMatchers.containsString( error.reference().toString() ) ) );
    }

    @Test
    public void shouldNotReportOOMErrors()
    {
        // Given
        AssertableLogProvider provider = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( provider.getLog( "userlog" ), new UsageData( mock( JobScheduler.class ) ) );

        Throwable cause = new OutOfMemoryError( "memory is fading" );
        Neo4jError error = Neo4jError.from( cause );

        // When
        reporter.report( error );

        // Then
        assertThat( error.status(), equalTo( (Status) Status.General.OutOfMemoryError ) );
        assertThat( error.message(),
                equalTo(  "There is not enough memory to perform the current task. Please try increasing " +
                          "'dbms.memory.heap.max_size' in the process wrapper configuration (normally in 'conf/neo4j-wrapper" +
                          ".conf' or, if you are using Neo4j Desktop, found through the user interface) or if you are running an embedded " +
                          "installation increase the heap by using '-Xmx' command line flag, and then restart the database." ) );
        provider.assertNoLoggingOccurred();
    }

    @Test
    public void shouldNotReportStackOverflowErrors()
    {
        // Given
        AssertableLogProvider provider = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( provider.getLog( "userlog" ), new UsageData( mock( JobScheduler.class ) ) );

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
                "in the process wrapper configuration (normally in 'conf/neo4j-wrapper.conf' or, if you are using " +
                "Neo4j Desktop, found through the user interface) or if you are running an embedded installation " +
                "just add -Xss2M as command line flag." ) );
        provider.assertNoLoggingOccurred();
    }
}