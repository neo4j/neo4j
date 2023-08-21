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
package org.neo4j.shell.commands;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.exception.CommandException;

class UseTest {
    private final DatabaseManager mockShell = mock(DatabaseManager.class);
    private final Command cmd = new Use(mockShell);

    @Test
    void setAbsentDatabaseOnNoArgument() throws CommandException {
        cmd.execute(List.of());

        verify(mockShell).setActiveDatabase(DatabaseManager.ABSENT_DB_NAME);
    }

    @Test
    void shouldFailIfMoreThanOneArg() {
        assertThatThrownBy(() -> cmd.execute(List.of("db1", "db2")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect number of arguments");
    }

    @Test
    void setActiveDatabase() throws CommandException {
        cmd.execute(List.of("db1"));

        verify(mockShell).setActiveDatabase("db1");
    }

    @Test
    void printUsage() {
        String usage = cmd.metadata().usage();
        assertThat(usage).isEqualTo("database");
    }

    @Test
    void setActiveDatabaseWithBackticks() throws CommandException {
        cmd.execute(List.of("`hello-world`"));
        verify(mockShell).setActiveDatabase("hello-world");
    }

    @Test
    void setActiveDatabaseWithoutBackticks() throws CommandException {
        cmd.execute(List.of("hello-world"));
        verify(mockShell).setActiveDatabase("hello-world");
    }
}
