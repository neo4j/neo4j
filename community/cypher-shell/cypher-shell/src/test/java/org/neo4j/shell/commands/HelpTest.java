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
package org.neo4j.shell.commands;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.commands.CommandHelper.CommandFactoryHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.printer.AnsiPrinter;
import org.neo4j.shell.printer.Printer;

class HelpTest {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();
    private final ByteArrayOutputStream err = new ByteArrayOutputStream();
    private Printer printer;
    private CommandFactoryHelper cmdHelper;
    private Command cmd;

    @BeforeEach
    public void setup() {
        out.reset();
        err.reset();
        printer = new AnsiPrinter(Format.VERBOSE, new PrintStream(out), new PrintStream(err), true);
        cmdHelper = mock(CommandFactoryHelper.class);
        cmd = new Help(printer, cmdHelper);
    }

    @Test
    void shouldAcceptNoArgs() {
        assertDoesNotThrow(() -> cmd.execute(List.of()));
    }

    @Test
    void shouldNotAcceptTooManyArgs() {
        assertThatThrownBy(() -> cmd.execute(List.of("bob", "alice")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Incorrect number of arguments");
    }

    @Test
    void helpListing() throws CommandException {
        // given
        var commands = List.of(mockFactory("bob"), mockFactory("bobby"));
        when(cmdHelper.factories()).thenReturn(commands);

        // when
        cmd.execute(List.of());

        // then
        assertEquals(
                """

                        Available commands:
                          [1mbob  [m description for bob
                          [1mbobby[m description for bobby

                        For help on a specific command type:
                            :help[1m command[m

                        Keyboard shortcuts:
                            Up and down arrows to access statement history.
                            Tab for autocompletion of commands, hit twice to select suggestion from list using arrow keys.

                        For help on cypher please visit:
                            https://neo4j.com/docs/cypher-manual/current/

                        """,
                out.toString(StandardCharsets.UTF_8));
        assertEquals(0, err.size());
    }

    @Test
    void helpForCommand() throws CommandException {
        // given
        var factory = mockFactory("bob");
        when(cmdHelper.factoryByName("bob")).thenReturn(factory);

        // when
        cmd.execute(List.of("bob"));

        // then
        assertEquals(
                """

                        usage: [1mbob[m usage for bob

                        help for bob

                        """,
                out.toString(StandardCharsets.UTF_8));
        assertEquals(0, err.size());
    }

    @Test
    void helpForNonExistingCommandThrows() {
        assertThatThrownBy(() -> cmd.execute(List.of("notacommandname")))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("No such command: notacommandname");
    }

    @Test
    void helpForCommandHasOptionalColon() throws CommandException {
        // given
        var factory = mockFactory(":bob");
        when(cmdHelper.factoryByName(":bob")).thenReturn(factory);

        // when
        cmd.execute(List.of("bob"));

        // then
        assertEquals(
                """

                        usage: [1m:bob[m usage for :bob

                        help for :bob

                        """,
                out.toString(StandardCharsets.UTF_8));
        assertEquals(0, err.size());
    }

    private static Command.Factory mockFactory(String name) {
        var metadata = new Command.Metadata(
                name, "description for " + name, "usage for " + name, "help for " + name, List.of());
        var factory = mock(Command.Factory.class);
        when(factory.metadata()).thenReturn(metadata);
        return factory;
    }
}
