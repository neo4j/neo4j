/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v1.runtime;

import org.junit.Test;

import java.util.UUID;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ErrorReporterTest
{
    @Test
    public void onlyDatabaseErrorsAreLogged()
    {
        AssertableLogProvider userLog = new AssertableLogProvider();
        AssertableLogProvider internalLog = new AssertableLogProvider();
        ErrorReporter reporter = newErrorReporter( userLog, internalLog );

        for ( Status.Classification classification : Status.Classification.values() )
        {
            if ( classification != Status.Classification.DatabaseError )
            {
                Status.Code code = newStatusCode( classification );
                Neo4jError error = Neo4jError.from( () -> code, "Database error" );
                reporter.report( error );

                userLog.assertNoLoggingOccurred();
                internalLog.assertNoLoggingOccurred();
            }
        }
    }

    @Test
    public void databaseErrorShouldLogFullMessageInDebugLogAndHelpfulPointerInUserLog()
    {
        // given
        AssertableLogProvider userLog = new AssertableLogProvider();
        AssertableLogProvider internalLog = new AssertableLogProvider();
        ErrorReporter reporter = newErrorReporter( userLog, internalLog );

        Neo4jError error = Neo4jError.fatalFrom( new TestDatabaseError() );
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

    private static ErrorReporter newErrorReporter( LogProvider userLog, LogProvider internalLog )
    {
        return new ErrorReporter( userLog.getLog( "userLog" ), internalLog.getLog( "internalLog" ) );
    }

    private static Status.Code newStatusCode( Status.Classification classification )
    {
        Status.Code code = mock( Status.Code.class );
        when( code.classification() ).thenReturn( classification );
        return code;
    }

    private static class TestDatabaseError extends RuntimeException implements Status.HasStatus
    {
        TestDatabaseError()
        {
            super( "Database error" );
        }

        @Override
        public Status status()
        {
            return () -> newStatusCode( Status.Classification.DatabaseError );
        }
    }
}
