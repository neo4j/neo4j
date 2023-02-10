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
package org.neo4j.bolt.protocol.common.fsm;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.neo4j.bolt.protocol.common.message.Error;
import org.neo4j.exceptions.SecurityAdministrationException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;

class StateMachineSPIImplTest {
    @Test
    void onlyDatabaseErrorsAreLogged() {
        var userLog = new AssertableLogProvider();
        var internalLog = new AssertableLogProvider();
        var logService = new SimpleLogService(userLog, internalLog);

        var spi = new StateMachineSPIImpl(logService);

        for (Status.Classification classification : Status.Classification.values()) {
            if (classification != Status.Classification.DatabaseError) {
                Status.Code code = newStatusCode(classification);
                Error error = Error.from(() -> code, "Database error");
                spi.reportError(error);

                assertThat(userLog).doesNotHaveAnyLogs();
                assertThat(internalLog).doesNotHaveAnyLogs();
            }
        }
    }

    @Test
    void databaseErrorShouldLogFullMessageInDebugLogAndHelpfulPointerInUserLog() {
        // given
        var userLog = new AssertableLogProvider();
        var internalLog = new AssertableLogProvider();
        var logService = new SimpleLogService(userLog, internalLog);

        var spi = new StateMachineSPIImpl(logService);

        Error error = Error.fatalFrom(new TestDatabaseError());
        UUID reference = error.reference();

        // when
        spi.reportError(error);

        // then
        assertThat(userLog)
                .containsMessages("Client triggered an unexpected error", reference.toString(), "Database error");

        assertThat(internalLog).containsMessages(reference.toString(), "Database error");
    }

    @Test
    void clientErrorShouldNotLog() {
        // given
        var userLog = new AssertableLogProvider();
        var internalLog = new AssertableLogProvider();
        var logService = new SimpleLogService(userLog, internalLog);

        var spi = new StateMachineSPIImpl(logService);

        Error error = Error.from(
                new SecurityAdministrationException("Unsupported administration command: CREATE DATABASE foo"));

        // when
        spi.reportError(error);

        // then
        assertThat(userLog).doesNotHaveAnyLogs();
        assertThat(internalLog).doesNotHaveAnyLogs();
    }

    private static Status.Code newStatusCode(Status.Classification classification) {
        Status.Code code = mock(Status.Code.class);
        when(code.classification()).thenReturn(classification);
        return code;
    }

    private static class TestDatabaseError extends RuntimeException implements Status.HasStatus {
        TestDatabaseError() {
            super("Database error");
        }

        @Override
        public Status status() {
            return () -> newStatusCode(Status.Classification.DatabaseError);
        }
    }
}
