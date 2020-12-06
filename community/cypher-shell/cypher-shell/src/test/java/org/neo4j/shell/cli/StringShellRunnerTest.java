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
package org.neo4j.shell.cli;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.shell.Historian;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.Logger;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.shell.cli.CliArgHelper.parse;

public class StringShellRunnerTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Logger logger = mock( Logger.class );
    private StatementExecuter statementExecuter = mock( StatementExecuter.class );

    @Test
    public void nullCypherShouldThrowException() throws IOException
    {
        thrown.expect( NullPointerException.class );
        thrown.expectMessage( "No cypher string specified" );

        new StringShellRunner( new CliArgs(), statementExecuter, logger );
    }

    @Test
    public void cypherShouldBePassedToRun() throws IOException, CommandException
    {
        String cypherString = "nonsense string";
        StringShellRunner runner = new StringShellRunner( parse( cypherString ), statementExecuter, logger );

        int code = runner.runUntilEnd();

        assertEquals( "Wrong exit code", 0, code );
        verify( statementExecuter ).execute( "nonsense string" );
        verifyNoMoreInteractions( statementExecuter );
    }

    @Test
    public void errorsShouldThrow() throws IOException, CommandException
    {
        ClientException kaboom = new ClientException( "Error kaboom" );
        doThrow( kaboom ).when( statementExecuter ).execute( anyString() );

        StringShellRunner runner = new StringShellRunner( parse( "nan anana" ), statementExecuter, logger );

        int code = runner.runUntilEnd();

        assertEquals( "Wrong exit code", 1, code );
        verify( logger ).printError( kaboom );
    }

    @Test
    public void shellRunnerHasNoHistory() throws Exception
    {
        // given
        StringShellRunner runner = new StringShellRunner( parse( "nan anana" ), statementExecuter, logger );

        // when then
        assertEquals( Historian.empty, runner.getHistorian() );
    }
}
