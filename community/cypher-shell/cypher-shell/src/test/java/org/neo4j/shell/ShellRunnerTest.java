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
package org.neo4j.shell;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.Test;
import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.NonInteractiveShellRunner;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.terminal.CypherShellTerminal;

class ShellRunnerTest {
    @Test
    void inputIsNonInteractiveIfForced() throws Exception {
        CliArgs args = new CliArgs();
        args.setNonInteractive(true);
        var terminal = mock(CypherShellTerminal.class);
        when(terminal.isInteractive()).thenReturn(true);
        ShellRunner runner =
                new ShellRunner.Factory().create(args, mock(CypherShell.class), mock(Printer.class), terminal);
        assertTrue(runner instanceof NonInteractiveShellRunner, "Should be non-interactive shell runner when forced");
    }
}
