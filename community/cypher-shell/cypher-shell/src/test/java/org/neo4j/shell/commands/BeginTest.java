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

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;

class BeginTest {
    private final TransactionHandler mockShell = mock(TransactionHandler.class);
    private final Command beginCommand = new Begin(mockShell);

    @Test
    void shouldNotAcceptArgs() {
        assertThatThrownBy(() -> beginCommand.execute(List.of("bob")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect number of arguments");
    }

    @Test
    void beginTransactionOnShell() throws CommandException {
        beginCommand.execute(List.of());
        verify(mockShell).beginTransaction();
    }
}
