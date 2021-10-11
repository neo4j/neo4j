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
package org.neo4j.shell;

import org.junit.jupiter.api.Test;

import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.NonInteractiveShellRunner;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.terminal.CypherShellTerminal;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class ShellRunnerTest
{
    private final ConnectionConfig connectionConfig = mock( ConnectionConfig.class );

    @Test
    void inputIsNonInteractiveIfForced() throws Exception
    {
        CliArgs args = new CliArgs();
        args.setNonInteractive( true );
        var terminal = mock( CypherShellTerminal.class );
        when( terminal.isInteractive() ).thenReturn( true );
        ShellRunner runner = new ShellRunner.Factory().create( args, mock( CypherShell.class ), mock( Logger.class ), connectionConfig, terminal );
        assertTrue( runner instanceof NonInteractiveShellRunner, "Should be non-interactive shell runner when forced" );
    }
}
