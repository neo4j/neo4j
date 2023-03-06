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

import org.neo4j.shell.QueryRunner;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;

import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

class ParamTest
{
    private QueryRunner db = mock( QueryRunner.class );
    private ParameterService parameters = ParameterService.create( db );
    private Command cmd = new Param( parameters );

    @BeforeEach
    void setup()
    {
        db = mock( QueryRunner.class );
        parameters = ParameterService.create( db );
        cmd = new Param( parameters );
    }

    @Test
    void setParams() throws CommandException
    {
        var param1 = new ParameterService.Parameter( "myParam", "'here I am'", "here I am" );
        var param2 = new ParameterService.Parameter( "myParam2", "2", 2L );
        var param3 = new ParameterService.Parameter( "myParam", "'again'", "again" );
        assertExecute( "myParam => 'here I am'", param1 );
        assertExecute( "myParam2 => 2", param1, param2 );
        assertExecute( "myParam => 'again'", param2, param3 );
    }

    @Test
    void shouldFailIfNoArgs()
    {
        Exception e = assertThrows( CommandException.class, () -> cmd.execute( "" ) );
        assertThat( e.getMessage(), containsString( "Incorrect usage" ) );
    }

    @Test
    void shouldFailIfOneArg()
    {
        Exception e = assertThrows( CommandException.class, () -> cmd.execute( "bob" ) );
        assertThat( e.getMessage(), containsString( "Incorrect usage.\nusage: :param name => <Cypher Expression>" ) );
    }

    @Test
    void shouldFailForVariablesWithoutEscaping()
    {
        Exception e = assertThrows( CommandException.class, () -> cmd.execute( "bob#   9" ) );
        assertThat( e.getMessage(), containsString( "Incorrect usage.\nusage: :param name => <Cypher Expression>" ) );
    }

    @Test
    void shouldFailForVariablesMixingMapStyleAssignmentAndLambdas()
    {
        Exception e = assertThrows( CommandException.class, () -> cmd.execute( "bob: => 9" ) );
        assertThat( e.getMessage(), containsString( "Incorrect usage" ) );
    }

    @Test
    void shouldFailForEmptyVariables()
    {
        Exception e = assertThrows( CommandException.class, () -> cmd.execute( "``   9" ) );
        assertThat( e.getMessage(), containsString( "Incorrect usage" ) );
    }

    @Test
    void shouldFailForInvalidVariables()
    {
        Exception e = assertThrows( CommandException.class, () -> cmd.execute( "`   9" ) );
        assertThat( e.getMessage(), containsString( "Incorrect usage" ) );
    }

    @Test
    void shouldFailForVariablesWithoutText()
    {
        Exception e = assertThrows( CommandException.class, () -> cmd.execute( "```   9" ) );
        assertThat( e.getMessage(), containsString( "Incorrect usage" ) );
    }

    @Test
    void printUsage()
    {
        assertThat( cmd.getUsage(), containsString( "name => <Cypher Expression>" ) );
    }

    private void assertExecute( String args, ParameterService.Parameter... expected ) throws CommandException
    {
        cmd.execute( args );
        var expectedMap = stream( expected ).collect( toMap( e -> e.name, identity() ) );
        assertEquals( expectedMap, parameters.parameters() );
        var expectedValues = stream( expected ).collect( toMap( e -> e.name, e -> e.value ) );
        assertEquals( expectedValues, parameters.parameterValues() );
    }
}
