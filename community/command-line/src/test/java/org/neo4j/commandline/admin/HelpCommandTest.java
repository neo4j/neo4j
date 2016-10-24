/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.commandline.admin;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HelpCommandTest
{
    @Mock
    private Consumer<String> out;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks( this );
    }

    @Test
    public void printsUnknownCommandWhenUnknownCommandIsProvided() throws Exception
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        when( commandLocator.getAllProviders() ).thenReturn( Collections.EMPTY_LIST );
        when( commandLocator.findProvider( "foobar" ) ).thenThrow( new NoSuchElementException( "foobar" ) );

        HelpCommand helpCommand = new HelpCommand( mock( Usage.class ), out, commandLocator );

        try
        {
            helpCommand.execute( "foobar" );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "Unknown command: foobar" ) );
        }
    }

    @Test
    public void printsAvailableCommandsWhenUnknownCommandIsProvided() throws Exception
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        ArrayList<AdminCommand.Provider> mockCommands = new ArrayList<AdminCommand.Provider>()
        {{
            add( mockCommand( "foo" ) );
            add( mockCommand( "bar" ) );
            add( mockCommand( "baz" ) );
        }};
        when( commandLocator.getAllProviders() ).thenReturn( mockCommands );
        when( commandLocator.findProvider( "foobar" ) ).thenThrow( new NoSuchElementException( "foobar" ) );

        HelpCommand helpCommand = new HelpCommand( mock( Usage.class ), out, commandLocator );

        try
        {
            helpCommand.execute( "foobar" );
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "Available commands are: foo bar baz" ) );
        }
    }

    private AdminCommand.Provider mockCommand( String name )
    {
        AdminCommand.Provider commandProvider = mock( AdminCommand.Provider.class );
        when( commandProvider.name() ).thenReturn( name );
        return commandProvider;
    }

    @Test
    public void showsArgumentsAndDescriptionForSpecifiedCommand() throws Exception
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        AdminCommand.Provider commandProvider = mock( AdminCommand.Provider.class );
        when( commandProvider.name() ).thenReturn( "foobar" );
        when( commandProvider.arguments() ).thenReturn( Optional.of( "--baz --qux" ) );
        when( commandProvider.description() ).thenReturn( "This is a description of the foobar command." );
        when( commandLocator.findProvider( "foobar" ) ).thenReturn( commandProvider );

        HelpCommand helpCommand = new HelpCommand( new Usage( "neo4j-admin", commandLocator ), out, commandLocator );
        helpCommand.execute( "foobar" );

        InOrder ordered = inOrder( out );
        ordered.verify( out ).accept( "neo4j-admin foobar --baz --qux" );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "This is a description of the foobar command." );
        ordered.verifyNoMoreInteractions();
    }
}
