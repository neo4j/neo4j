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
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.Historian;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.AnsiPrinter;
import org.neo4j.shell.state.BoltStateHandler;

class CommandHelperTest {
    private final AnsiPrinter logger = new AnsiPrinter();
    private final ParameterService parameters = mock(ParameterService.class);
    private final BoltStateHandler boltStateHandler = mock(BoltStateHandler.class);
    private final CypherShell shell =
            new CypherShell(logger, boltStateHandler, new PrettyPrinter(PrettyConfig.DEFAULT), parameters);

    @Test
    void shouldIgnoreCaseForCommands() {
        // Given
        CommandHelper commandHelper = new CommandHelper(logger, Historian.empty, shell, null, null);

        // When
        Command begin = commandHelper.getCommand(":BEGIN");

        // Then
        assertThat(begin).isInstanceOf(Begin.class);
    }

    @Test
    void internalStateSanityTest() {
        var args = new Command.Factory.Arguments(logger, Historian.empty, shell, null, null);
        var factories = new CommandHelper.CommandFactoryHelper().factoryByClass();
        assertThat(factories)
                .allSatisfy((cls, factory) -> assertThat(factory.executor(args)).isExactlyInstanceOf(cls));
    }
}
