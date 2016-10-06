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
package org.neo4j.bolt.v1.runtime;

import org.junit.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;

import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorReporterTest
{
    @Test
    public void clientErrorShouldNotLog() throws Exception
    {
        // given
        AssertableLogProvider userLog = new AssertableLogProvider();
        AssertableLogProvider internalLog = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( userLog.getLog( "userLog" ), internalLog.getLog( "internalLog" ) );

        Status.Code code = mock( Status.Code.class );
        Neo4jError error = Neo4jError.from( () -> code, "Should not be logged" );
        when( code.classification() ).thenReturn( Status.Classification.ClientError );

        // when
        reporter.report( error );

        // then

        userLog.assertNoLoggingOccurred();
        internalLog.assertNoLoggingOccurred();
    }

    @Test
    public void clientNotificationShouldNotLog() throws Exception
    {
        // given
        AssertableLogProvider userLog = new AssertableLogProvider();
        AssertableLogProvider internalLog = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( userLog.getLog( "userLog" ), internalLog.getLog( "internalLog" ) );

        Status.Code code = mock( Status.Code.class );
        Neo4jError error = Neo4jError.from( () -> code, "Should not be logged" );
        when( code.classification() ).thenReturn( Status.Classification.ClientNotification );

        // when
        reporter.report( error );

        // then

        userLog.assertNoLoggingOccurred();
        internalLog.assertNoLoggingOccurred();
    }

    @Test
    public void databaseErrorShouldLogFullMessageInDebugLogAndHelpfulPointerInUserLog() throws Exception
    {
        // given
        AssertableLogProvider userLog = new AssertableLogProvider();
        AssertableLogProvider internalLog = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( userLog.getLog( "userLog" ), internalLog.getLog( "internalLog" ) );

        Status.Code code = mock( Status.Code.class );
        Neo4jError error = Neo4jError.from( () -> code, "Database error" );
        when( code.classification() ).thenReturn( Status.Classification.DatabaseError );
        UUID reference = error.reference();

        // when
        reporter.report( error );

        // then
        userLog.assertContainsLogCallContaining( "Client triggered an unexpected error" );
        userLog.assertContainsLogCallContaining( reference.toString() );
        userLog.assertContainsLogCallContaining( "Database error" );

        internalLog.assertContainsLogCallContaining( reference.toString() );
        internalLog.assertContainsLogCallContaining( "Database error" );
    }

    @Test
    public void transientErrorShouldLogFullMessageInDebugLogAndHelpfulPointerInUserLog() throws Exception
    {
        // given
        AssertableLogProvider userLog = new AssertableLogProvider();
        AssertableLogProvider internalLog = new AssertableLogProvider();
        ErrorReporter reporter =
                new ErrorReporter( userLog.getLog( "userLog" ), internalLog.getLog( "internalLog" ) );

        Status.Code code = mock( Status.Code.class );
        Neo4jError error = Neo4jError.from( () -> code, "Transient error" );
        when( code.classification() ).thenReturn( Status.Classification.TransientError );
        UUID reference = error.reference();

        // when
        reporter.report( error );

        // then
        userLog.assertContainsLogCallContaining( "Client triggered an unexpected error" );
        userLog.assertContainsLogCallContaining( reference.toString() );
        userLog.assertContainsLogCallContaining( "Transient error" );

        internalLog.assertContainsLogCallContaining( reference.toString() );
        internalLog.assertContainsLogCallContaining( "Transient error" );
    }
}
