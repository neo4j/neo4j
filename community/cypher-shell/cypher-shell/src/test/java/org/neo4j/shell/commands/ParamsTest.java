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

import java.util.HashMap;

import org.neo4j.shell.QueryRunner;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.state.ParamValue;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class ParamsTest
{
    private QueryRunner queryRunner;
    private ParameterService parameters;
    private Logger logger;
    private Params cmd;

    @BeforeEach
    void setup() throws CommandException
    {
        queryRunner = mock(QueryRunner.class);
        parameters = ParameterService.create( queryRunner );
        logger = mock( Logger.class );
        cmd = new Params( logger, parameters );
    }

    @Test
    void descriptionNotNull()
    {
        assertNotNull( cmd.getDescription() );
    }

    @Test
    void usageNotNull()
    {
        assertNotNull( cmd.getUsage() );
    }

    @Test
    void helpNotNull()
    {
        assertNotNull( cmd.getHelp() );
    }

    @Test
    void runCommand() throws CommandException
    {
        // given
        parameters.setParameter( new ParameterService.Parameter( "var", "9", 9 ) );
        // when
        cmd.execute( "" );
        // then
        verify( logger ).printOut( ":param var => 9" );
        verifyNoMoreInteractions( logger );
    }

    @Test
    void runCommandAlignment() throws CommandException
    {
        // given
        parameters.setParameter( new ParameterService.Parameter( "param", "99999", 99999 ) );
        parameters.setParameter( new ParameterService.Parameter( "var", "9", 9 ) );
        parameters.setParameter( new ParameterService.Parameter( "param", "99998", 99998 ) );
        // when
        cmd.execute( "" );
        // then
        verify( logger ).printOut( ":param param => 99998" );
        verify( logger ).printOut( ":param var   => 9" );
        verifyNoMoreInteractions( logger );
    }

    @Test
    void runCommandWithArg() throws CommandException
    {
        // given
        parameters.setParameter( new ParameterService.Parameter( "var", "9", 9 ) );
        parameters.setParameter( new ParameterService.Parameter( "param", "9999", 9999 ) );
        // when
        cmd.execute( "var" );
        // then
        verify( logger ).printOut( ":param var => 9" );
        verifyNoMoreInteractions( logger );
    }

    @Test
    void runCommandWithArgWithExtraSpace() throws CommandException
    {
        // given
        parameters.setParameter( new ParameterService.Parameter( "var", "9", 9 ) );
        parameters.setParameter( new ParameterService.Parameter( "param", "9999", 9999 ) );
        // when
        cmd.execute( " var" );
        // then
        verify( logger ).printOut( ":param var => 9" );
        verifyNoMoreInteractions( logger );
    }

    @Test
    void runCommandWithArgWithBackticks() throws CommandException
    {
        // given
        parameters.setParameter( new ParameterService.Parameter( "var", "9", 9 ) );
        parameters.setParameter( new ParameterService.Parameter( "param", "9999", 9999 ) );
        // when
        cmd.execute( "`var`" );
        // then
        verify( logger ).printOut( ":param `var` => 9" );
        verifyNoMoreInteractions( logger );
    }

    @Test
    void runCommandWithSpecialCharacters() throws CommandException
    {
        // given
        parameters.setParameter( new ParameterService.Parameter( "var `", "9", 9 ) );
        parameters.setParameter( new ParameterService.Parameter( "param", "9999", 9999 ) );
        // when
        cmd.execute( "`var ```" );
        // then
        verify( logger ).printOut( ":param `var ``` => 9" );
        verifyNoMoreInteractions( logger );
    }

    @Test
    void runCommandWithUnknownArg()
    {
        // given
        parameters.setParameter( new ParameterService.Parameter( "var", "9", 9 ) );

        // when
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "bob" ) );
        assertThat( exception.getMessage(), containsString( "Unknown parameter: bob" ) );
    }

    @Test
    void shouldNotAcceptMoreThanOneArgs()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "bob sob" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }
}
