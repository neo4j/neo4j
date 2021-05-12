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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class HelpTest
{
    private final Logger logger = mock( Logger.class );
    private final CommandHelper cmdHelper = mock( CommandHelper.class );
    private final Command cmd = new Help( logger, cmdHelper );

    @Test
    void shouldAcceptNoArgs()
    {
        assertDoesNotThrow( () -> cmd.execute( "" ) );
    }

    @Test
    void shouldNotAcceptTooManyArgs()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "bob alice" ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void helpListing() throws CommandException
    {
        // given
        List<Command> commandList = new ArrayList<>();

        commandList.add( new FakeCommand( "bob" ) );
        commandList.add( new FakeCommand( "bobby" ) );

        doReturn( commandList ).when( cmdHelper ).getAllCommands();

        // when
        cmd.execute( "" );

        // then
        verify( logger ).printOut( "\nAvailable commands:" );
        verify( logger ).printOut( "  @|BOLD bob  |@ description for bob" );
        verify( logger ).printOut( "  @|BOLD bobby|@ description for bobby" );
        verify( logger ).printOut( "\nFor help on a specific command type:" );
        verify( logger ).printOut( "    :help@|BOLD  command|@\n" );
        verify( logger ).printOut( "\nFor help on cypher please visit:" );
        verify( logger ).printOut( "    " + Help.CYPHER_REFCARD_LINK + "\n" );
    }

    @Test
    void helpForCommand() throws CommandException
    {
        // given
        doReturn( new FakeCommand( "bob" ) ).when( cmdHelper ).getCommand( eq( "bob" ) );

        // when
        cmd.execute( "bob" );

        // then
        verify( logger ).printOut( "\nusage: @|BOLD bob|@ usage for bob\n"
                                   + "\nhelp for bob\n" );
    }

    @Test
    void helpForNonExistingCommandThrows()
    {
        CommandException exception = assertThrows( CommandException.class, () -> cmd.execute( "notacommandname" ) );
        assertThat( exception.getMessage(), containsString( "No such command: notacommandname" ) );
    }

    @Test
    void helpForCommandHasOptionalColon() throws CommandException
    {
        // given
        doReturn( new FakeCommand( ":bob" ) ).when( cmdHelper ).getCommand( eq( ":bob" ) );

        // when
        cmd.execute( "bob" );

        // then
        verify( logger ).printOut( "\nusage: @|BOLD :bob|@ usage for :bob\n"
                                   + "\nhelp for :bob\n" );
    }

    private static class FakeCommand implements Command
    {
        private final String name;

        FakeCommand( String name )
        {
            this.name = name;
        }

        @Nonnull
        @Override
        public String getName()
        {
            return name;
        }

        @Nonnull
        @Override
        public String getDescription()
        {
            return "description for " + name;
        }

        @Nonnull
        @Override
        public String getUsage()
        {
            return "usage for " + name;
        }

        @Nonnull
        @Override
        public String getHelp()
        {
            return "help for " + name;
        }

        @Nonnull
        @Override
        public List<String> getAliases()
        {
            return Collections.emptyList();
        }

        @Override
        public void execute( @Nonnull String args ) throws ExitException, CommandException
        {

        }
    }
}
