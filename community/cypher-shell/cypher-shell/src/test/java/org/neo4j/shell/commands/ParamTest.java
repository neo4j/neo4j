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

import org.neo4j.shell.ParameterMap;
import org.neo4j.shell.exception.CommandException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.hamcrest.CoreMatchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ParamTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private ParameterMap mockShell = mock( ParameterMap.class );
    private Command cmd;

    @Before
    public void setup()
    {
        this.cmd = new Param( mockShell );
    }

    @Test
    public void shouldFailIfNoArgs() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        cmd.execute( "" );
        fail( "Expected error" );
    }

    @Test
    public void shouldFailIfOneArg() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        cmd.execute( "bob" );
        fail( "Expected error" );
    }

    @Test
    public void setParam() throws CommandException
    {
        cmd.execute( "bob   9" );

        verify( mockShell ).setParameter( "bob", "9" );
    }

    @Test
    public void setLambdasAsParam() throws CommandException
    {
        cmd.execute( "bob => 9" );

        verify( mockShell ).setParameter( "bob", "9" );
    }

    @Test
    public void setLambdasAsParamWithBackticks() throws CommandException
    {
        cmd.execute( "`bob` => 9" );

        verify( mockShell ).setParameter( "`bob`", "9" );
    }

    @Test
    public void setSpecialCharacterParameter() throws CommandException
    {
        cmd.execute( "bØb   9" );

        verify( mockShell ).setParameter( "bØb", "9" );
    }

    @Test
    public void setSpecialCharacterParameterForLambdaExpressions() throws CommandException
    {
        cmd.execute( "`first=>Name` => \"Bruce\"" );

        verify( mockShell ).setParameter( "`first=>Name`", "\"Bruce\"" );
    }

    @Test
    public void setParamWithSpecialCharacters() throws CommandException
    {
        cmd.execute( "`bob#`   9" );

        verify( mockShell ).setParameter( "`bob#`", "9" );
    }

    @Test
    public void setParamWithOddNoOfBackTicks() throws CommandException
    {
        cmd.execute( " `bo `` sömething ```   9" );

        verify( mockShell ).setParameter( "`bo `` sömething ```", "9" );
    }

    @Test
    public void shouldFailForVariablesWithoutEscaping() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        cmd.execute( "bob#   9" );

        fail( "Expected error" );
    }

    @Test
    public void shouldFailForVariablesMixingMapStyleAssignmentAndLambdas() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect usage" ) );

        cmd.execute( "bob: => 9" );

        fail( "Expected error" );
    }

    @Test
    public void shouldFailForEmptyVariables() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        cmd.execute( "``   9" );

        fail( "Expected error" );
    }

    @Test
    public void shouldFailForInvalidVariables() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        cmd.execute( "`   9" );

        fail( "Expected error" );
    }

    @Test
    public void shouldFailForVariablesWithoutText() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( containsString( "Incorrect number of arguments" ) );

        cmd.execute( "```   9" );

        fail( "Expected error" );
    }

    @Test
    public void shouldNotSplitOnSpace() throws CommandException
    {
        cmd.execute( "bob 'one two'" );
        verify( mockShell ).setParameter( "bob", "'one two'" );
    }

    @Test
    public void shouldAcceptUnicodeAlphaNumeric() throws CommandException
    {
        cmd.execute( "böb 'one two'" );
        verify( mockShell ).setParameter( "böb", "'one two'" );
    }

    @Test
    public void shouldAcceptColonFormOfParams() throws CommandException
    {
        cmd.execute( "bob: one" );
        verify( mockShell ).setParameter( "bob", "one" );
    }

    @Test
    public void shouldAcceptForTwoColonsFormOfParams() throws CommandException
    {
        cmd.execute( "`bob:`: one" );
        verify( mockShell ).setParameter( "`bob:`", "one" );

        cmd.execute( "`t:om` two" );
        verify( mockShell ).setParameter( "`t:om`", "two" );
    }

    @Test
    public void shouldNotExecuteEscapedCypher() throws CommandException
    {
        cmd.execute( "bob \"RETURN 5 as bob\"" );
        verify( mockShell ).setParameter( "bob", "\"RETURN 5 as bob\"" );
    }

    @Test
    public void printUsage() throws CommandException
    {
        String usage = cmd.getUsage();
        assertEquals( usage, "name => value" );
    }
}
