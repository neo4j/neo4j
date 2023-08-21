/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.internal;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.logging.AssertableLogProvider.Level.ERROR;
import static org.neo4j.logging.LogAssertions.assertThat;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.monitoring.DatabaseHealthEventGenerator;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.monitoring.OutOfDiskSpace;
import org.neo4j.monitoring.Panic;

class DatabaseHealthTest {
    @Test
    void shouldGenerateDatabasePanicEvents() {
        // GIVEN
        DatabaseHealthEventGenerator generator = mock(DatabaseHealthEventGenerator.class);
        Panic databasePanic =
                new DatabaseHealth(generator, NullLogProvider.getInstance().getLog(DatabaseHealth.class));

        // WHEN
        Exception cause = new Exception("My own fault");
        databasePanic.panic(cause);
        databasePanic.panic(cause);

        // THEN
        verify(generator).panic(cause);
    }

    @Test
    void shouldLogDatabasePanicEvent() {
        // GIVEN
        AssertableLogProvider logProvider = new AssertableLogProvider();
        Panic databasePanic =
                new DatabaseHealth(mock(DatabaseHealthEventGenerator.class), logProvider.getLog(DatabaseHealth.class));

        // WHEN
        String message = "Listen everybody... panic!";
        Exception exception = new Exception(message);
        databasePanic.panic(exception);

        // THEN
        assertThat(logProvider)
                .forClass(DatabaseHealth.class)
                .forLevel(ERROR)
                .containsMessageWithException(
                        "Database panic: The database has encountered a critical error, "
                                + "and needs to be restarted. Please see database logs for more details.",
                        exception);
    }

    @Test
    void shouldLogDatabaseOutOfDiskSpaceEvent() {
        // GIVEN
        AssertableLogProvider logProvider = new AssertableLogProvider();
        OutOfDiskSpace databaseOutOfDiskSpace =
                new DatabaseHealth(mock(DatabaseHealthEventGenerator.class), logProvider.getLog(DatabaseHealth.class));

        // WHEN
        String message = "Listen everybody... out of disk space!";
        Exception exception = new Exception(message);
        databaseOutOfDiskSpace.outOfDiskSpace(exception);

        // THEN
        assertThat(logProvider)
                .forClass(DatabaseHealth.class)
                .forLevel(ERROR)
                .containsMessageWithException("The database was unable to allocate enough disk space.", exception);
    }
}
