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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import org.neo4j.shell.ParameterMap;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ParameterException;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class ParamTest
{
    private final ParameterMap mockShell = mock( ParameterMap.class );
    private final Command cmd = new Param( mockShell );

    @Test
    void shouldFailIfNoArgs()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void shouldFailIfOneArg()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "bob" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void setParam() throws ParameterException, CommandException
    {
        cmd.execute( "bob   9" );

        verify( mockShell ).setParameter( "bob", "9" );
    }

    @Test
    void setLambdasAsParam() throws ParameterException, CommandException
    {
        cmd.execute( "bob => 9" );

        verify( mockShell ).setParameter( "bob", "9" );
    }

    @Test
    void setLambdasAsParamWithBackticks() throws ParameterException, CommandException
    {
        cmd.execute( "`bob` => 9" );

        verify( mockShell ).setParameter( "`bob`", "9" );
    }

    @Test
    void setSpecialCharacterParameter() throws ParameterException, CommandException
    {
        cmd.execute( "bØb   9" );

        verify( mockShell ).setParameter( "bØb", "9" );
    }

    @Test
    void setSpecialCharacterParameterForLambdaExpressions() throws ParameterException, CommandException
    {
        cmd.execute( "`first=>Name` => \"Bruce\"" );

        verify( mockShell ).setParameter( "`first=>Name`", "\"Bruce\"" );
    }

    @Test
    void setParamWithSpecialCharacters() throws ParameterException, CommandException
    {
        cmd.execute( "`bob#`   9" );

        verify( mockShell ).setParameter( "`bob#`", "9" );
    }

    @Test
    void setParamWithOddNoOfBackTicks() throws ParameterException, CommandException
    {
        cmd.execute( " `bo `` sömething ```   9" );

        verify( mockShell ).setParameter( "`bo `` sömething ```", "9" );
    }

    @Test
    void shouldFailForVariablesWithoutEscaping()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "bob#   9" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void shouldFailForVariablesMixingMapStyleAssignmentAndLambdas()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "bob: => 9" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect usage" ) );
    }

    @Test
    void shouldFailForEmptyVariables()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "``   9" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void shouldFailForInvalidVariables()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "`   9" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void shouldFailForVariablesWithoutText()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "```   9" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void shouldNotSplitOnSpace() throws ParameterException, CommandException
    {
        cmd.execute( "bob 'one two'" );
        verify( mockShell ).setParameter( "bob", "'one two'" );
    }

    @Test
    void shouldAcceptUnicodeAlphaNumeric() throws ParameterException, CommandException
    {
        cmd.execute( "böb 'one two'" );
        verify( mockShell ).setParameter( "böb", "'one two'" );
    }

    @Test
    void shouldAcceptColonFormOfParams() throws ParameterException, CommandException
    {
        cmd.execute( "bob: one" );
        verify( mockShell ).setParameter( "bob", "one" );
    }

    @Test
    void shouldAcceptForTwoColonsFormOfParams() throws ParameterException, CommandException
    {
        cmd.execute( "`bob:`: one" );
        verify( mockShell ).setParameter( "`bob:`", "one" );

        cmd.execute( "`t:om` two" );
        verify( mockShell ).setParameter( "`t:om`", "two" );
    }

    @Test
    void shouldNotExecuteEscapedCypher() throws ParameterException, CommandException
    {
        cmd.execute( "bob \"RETURN 5 as bob\"" );
        verify( mockShell ).setParameter( "bob", "\"RETURN 5 as bob\"" );
    }

    @Test
    void printUsage()
    {
        String usage = cmd.getUsage();
        assertEquals( usage, "name => value" );
    }
}
