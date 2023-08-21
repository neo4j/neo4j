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
package org.neo4j.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.test.AsyncDatabaseOperation.findDatabaseEventually;

import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.graphdb.GraphDatabaseService;

class AsyncDatabaseOperationTest {

    public static final String DB = "foo";
    public static final Duration TIMEOUT = Duration.ofSeconds(1);

    @Test
    void shouldThrowAtTheEnd() {
        var managementService = mock(DatabaseManagementService.class);
        when(managementService.database(DB)).thenThrow(new DatabaseNotFoundException());

        assertThat(assertThrows(
                        DatabaseNotFoundException.class, () -> findDatabaseEventually(managementService, DB, TIMEOUT)))
                .hasMessageContaining(DB);
        verify(managementService, atLeastOnce()).database(DB);
    }

    @Test
    void shouldThrowAtTheEndWhenNotAvailable() {
        var managementService = mock(DatabaseManagementService.class);
        var database = mock(GraphDatabaseService.class);
        when(database.isAvailable()).thenReturn(false);
        when(managementService.database(DB))
                .thenThrow(new DatabaseNotFoundException())
                .thenReturn(database);

        assertThat(assertThrows(
                        DatabaseNotFoundException.class, () -> findDatabaseEventually(managementService, DB, TIMEOUT)))
                .hasMessageContaining(DB);
        verify(managementService, atLeastOnce()).database(DB);
    }

    @Test
    void shouldReturnIfFoundAndAvailable() {
        var managementService = mock(DatabaseManagementService.class);
        var unavaliableDatabase = mock(GraphDatabaseService.class);
        var availableDatabase = mock(GraphDatabaseService.class);
        when(unavaliableDatabase.isAvailable()).thenReturn(false);
        when(availableDatabase.isAvailable()).thenReturn(true);
        when(managementService.database(DB))
                .thenThrow(new DatabaseNotFoundException())
                .thenReturn(unavaliableDatabase)
                .thenReturn(availableDatabase);

        assertThat(findDatabaseEventually(managementService, DB, TIMEOUT)).isSameAs(availableDatabase);
        verify(managementService, times(3)).database(DB);
    }
}
