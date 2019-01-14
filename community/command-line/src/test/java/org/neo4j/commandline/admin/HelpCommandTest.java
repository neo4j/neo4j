/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.commandline.admin;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.neo4j.commandline.arguments.Arguments;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
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
    @SuppressWarnings( "unchecked" )
    public void printsUnknownCommandWhenUnknownCommandIsProvided()
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        when( commandLocator.getAllProviders() ).thenReturn( Collections.EMPTY_LIST );
        when( commandLocator.findProvider( "foobar" ) ).thenThrow( new NoSuchElementException( "foobar" ) );

        HelpCommand helpCommand = new HelpCommand( mock( Usage.class ), out, commandLocator );

        try
        {
            helpCommand.execute( "foobar" );
            fail();
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "Unknown command: foobar" ) );
        }
    }

    @Test
    public void printsAvailableCommandsWhenUnknownCommandIsProvided()
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
            fail();
        }
        catch ( IncorrectUsage e )
        {
            assertThat( e.getMessage(), containsString( "Available commands are: foo bar baz" ) );
        }
    }

    @Test
    public void testAdminUsage() throws Exception
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        ArrayList<AdminCommand.Provider> mockCommands = new ArrayList<AdminCommand.Provider>()
        {{
            add( mockCommand( "foo" ) );
            add( mockCommand( "bar" ) );
            add( mockCommand( "baz" ) );
        }};
        when( commandLocator.getAllProviders() ).thenReturn( mockCommands );

        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            Usage usage = new Usage( "neo4j-admin", commandLocator );

            HelpCommand helpCommand = new HelpCommand( usage, ps::println, commandLocator );

            helpCommand.execute();

            assertEquals( String.format( "usage: neo4j-admin <command>%n" +
                            "%n" +
                            "Manage your Neo4j instance.%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "available commands:%n" +
                            "%n" +
                            "General%n" +
                            "    bar%n" +
                            "        null%n" +
                            "    baz%n" +
                            "        null%n" +
                            "    foo%n" +
                            "        null%n" +
                            "%n" +
                            "Use neo4j-admin help <command> for more details.%n" ),
                    baos.toString() );
        }
    }

    @Test
    public void showsArgumentsAndDescriptionForSpecifiedCommand() throws Exception
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        AdminCommand.Provider commandProvider = mock( AdminCommand.Provider.class );
        when( commandProvider.name() ).thenReturn( "foobar" );
        Arguments arguments = new Arguments().withDatabase();
        when( commandProvider.allArguments() ).thenReturn( arguments );
        when( commandProvider.possibleArguments() ).thenReturn( Collections.singletonList( arguments ) );
        when( commandProvider.description() ).thenReturn( "This is a description of the foobar command." );
        when( commandLocator.findProvider( "foobar" ) ).thenReturn( commandProvider );

        try ( ByteArrayOutputStream baos = new ByteArrayOutputStream() )
        {
            PrintStream ps = new PrintStream( baos );

            HelpCommand helpCommand = new HelpCommand( new Usage( "neo4j-admin", commandLocator ),
                    ps::println, commandLocator );
            helpCommand.execute( "foobar" );

            assertEquals( String.format( "usage: neo4j-admin foobar [--database=<name>]%n" +
                            "%n" +
                            "environment variables:%n" +
                            "    NEO4J_CONF    Path to directory which contains neo4j.conf.%n" +
                            "    NEO4J_DEBUG   Set to anything to enable debug output.%n" +
                            "    NEO4J_HOME    Neo4j home directory.%n" +
                            "    HEAP_SIZE     Set JVM maximum heap size during command execution.%n" +
                            "                  Takes a number and a unit, for example 512m.%n" +
                            "%n" +
                            "This is a description of the foobar command.%n" +
                            "%n" +
                            "options:%n" +
                            "  --database=<name>   Name of database. [default:graph.db]%n" ),
                    baos.toString() );
        }
    }

    private AdminCommand.Provider mockCommand( String name )
    {
        AdminCommand.Provider commandProvider = mock( AdminCommand.Provider.class );
        when( commandProvider.name() ).thenReturn( name );
        when( commandProvider.commandSection() ).thenReturn( AdminCommandSection.general() );
        return commandProvider;
    }
}
