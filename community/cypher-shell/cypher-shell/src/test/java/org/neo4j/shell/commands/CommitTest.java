/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.exception.CommandException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CommitTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();
    private Command commitCommand;
    private TransactionHandler mockShell = mock( TransactionHandler.class );

    @Before
    public void setup()
    {
        this.commitCommand = new Commit( mockShell );
    }

    @Test
    public void shouldNotAcceptArgs() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        commitCommand.execute( "bob" );
    }

    @Test
    public void commitTransactionOnShell() throws CommandException
    {
        commitCommand.execute( "" );

        verify( mockShell ).commitTransaction();
    }
}
