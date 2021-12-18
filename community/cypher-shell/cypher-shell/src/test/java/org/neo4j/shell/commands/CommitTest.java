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

import org.junit.jupiter.api.Test;

import java.util.List;

import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class CommitTest
{
    private final TransactionHandler mockShell = mock( TransactionHandler.class );
    private final Command commitCommand = new Commit( mockShell );

    @Test
    void shouldNotAcceptArgs()
    {
        CommandException exception = assertThrows( CommandException.class, () -> commitCommand.execute( List.of( "bob" ) ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void commitTransactionOnShell() throws CommandException
    {
        commitCommand.execute( List.of() );
        verify( mockShell ).commitTransaction();
    }
}
