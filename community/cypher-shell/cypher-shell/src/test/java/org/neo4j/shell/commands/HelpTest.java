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
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.commands.CommandHelper.CommandFactoryHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.printer.Printer;

class HelpTest {
    private final Printer printer = mock(Printer.class);
    private CommandFactoryHelper cmdHelper;
    private Command cmd;

    @BeforeEach
    public void setup() {
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
        verify(printer).printOut("\nAvailable commands:");
        verify(printer).printOut("  @|BOLD bob  |@ description for bob");
        verify(printer).printOut("  @|BOLD bobby|@ description for bobby");
        verify(printer).printOut("\nFor help on a specific command type:");
        verify(printer).printOut("    :help@|BOLD  command|@\n");
        verify(printer).printOut("\nFor help on cypher please visit:");
        verify(printer).printOut("    " + Help.CYPHER_MANUAL_LINK + "\n");
    }

    @Test
    void helpForCommand() throws CommandException {
        // given
        var factory = mockFactory("bob");
        when(cmdHelper.factoryByName("bob")).thenReturn(factory);

        // when
        cmd.execute(List.of("bob"));

        // then
        verify(printer).printOut("\nusage: @|BOLD bob|@ usage for bob\n" + "\nhelp for bob\n");
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
        verify(printer).printOut("\nusage: @|BOLD :bob|@ usage for :bob\n" + "\nhelp for :bob\n");
    }

    private static Command.Factory mockFactory(String name) {
        var metadata = new Command.Metadata(
                name, "description for " + name, "usage for " + name, "help for " + name, List.of());
        var factory = mock(Command.Factory.class);
        when(factory.metadata()).thenReturn(metadata);
        return factory;
    }
}
