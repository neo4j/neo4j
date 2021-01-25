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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

import org.neo4j.shell.Historian;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.Logger;

import static org.junit.jupiter.api.Assertions.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class HistoryTest
{

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private Logger logger = mock( Logger.class );
    private Historian historian = mock( Historian.class );
    private Command cmd;

    @Before
    public void setup()
    {
        this.cmd = new History( logger, historian );
    }

    @Test
    public void shouldNotAcceptArgs() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        cmd.execute( "bob" );
        fail( "Should not accept args" );
    }

    @Test
    public void shouldPrintHistoryCorrectlyNumberedFrom1() throws CommandException
    {
        when( historian.getHistory() ).thenReturn( Arrays.asList( ":help", ":exit" ) );

        cmd.execute( "" );

        verify( logger ).printOut( eq( " 1  :help\n" +
                                       " 2  :exit\n" ) );
    }
}
