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
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.FileNotFoundException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser.CypherStatement;

class SourceTest {
    private final CypherShell shell = mock(CypherShell.class);
    private final Source cmd = new Source(shell, new ShellStatementParser());

    @Test
    void descriptionNotNull() {
        assertThat(cmd.metadata().description()).isNotNull();
    }

    @Test
    void usageNotNull() {
        assertThat(cmd.metadata().usage()).isNotNull();
    }

    @Test
    void helpNotNull() {
        assertThat(cmd.metadata().help()).isNotNull();
    }

    @Test
    void runCommand() throws CommandException {
        // given
        cmd.execute(List.of(fileFromResource("test.cypher")));
        verify(shell).execute(List.of(new CypherStatement("RETURN 42", true, 0, 8)));
        verifyNoMoreInteractions(shell);
    }

    @Test
    void shouldFailIfFileNotThere() {
        assertThatThrownBy(() -> cmd.execute(List.of("not.there")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Cannot find file: 'not.there'")
                .hasCauseInstanceOf(FileNotFoundException.class);
    }

    @Test
    void shouldNotAcceptMoreThanOneArgs() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob", "sob")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect number of arguments");
    }

    @Test
    void shouldNotAcceptZeroArgs() {
        assertThatThrownBy(() -> cmd.execute(List.of()))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect number of arguments");
    }

    @Test
    void shouldTryToExecuteIncompleteStatements() throws CommandException {
        cmd.execute(List.of(fileFromResource("invalid.cypher")));
        verify(shell)
                .execute(List.of(
                        new CypherStatement("INVALID CYPHER\nWITHOUT SEMICOLON\n// Comment at end", false, 0, 49)));
        verifyNoMoreInteractions(shell);
    }

    private String fileFromResource(String filename) {
        return getClass().getResource(filename).getFile();
    }
}
