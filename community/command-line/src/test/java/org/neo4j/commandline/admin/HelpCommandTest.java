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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.neo4j.commandline.arguments.Arguments;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HelpCommandTest
{
    @Mock
    private Consumer<String> out;

    @BeforeEach
    void setUp()
    {
        MockitoAnnotations.initMocks( this );
    }

    @Test
    void printsUnknownCommandWhenUnknownCommandIsProvided()
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        when( commandLocator.getAllProviders() ).thenReturn( Collections.emptyList() );
        when( commandLocator.findProvider( "foobar" ) ).thenThrow( new NoSuchElementException( "foobar" ) );

        HelpCommand helpCommand = new HelpCommand( mock( Usage.class ), out, commandLocator );

        IncorrectUsage incorrectUsage = assertThrows( IncorrectUsage.class, () -> helpCommand.execute( "foobar" ) );
        assertThat( incorrectUsage.getMessage(), containsString( "Unknown command: foobar" ) );
    }

    @Test
    void printsAvailableCommandsWhenUnknownCommandIsProvided()
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        List<AdminCommand.Provider> mockCommands = asList( mockCommand( "foo" ), mockCommand( "bar" ), mockCommand( "baz" ) );
        when( commandLocator.getAllProviders() ).thenReturn( mockCommands );
        when( commandLocator.findProvider( "foobar" ) ).thenThrow( new NoSuchElementException( "foobar" ) );

        HelpCommand helpCommand = new HelpCommand( mock( Usage.class ), out, commandLocator );

        IncorrectUsage incorrectUsage = assertThrows( IncorrectUsage.class, () -> helpCommand.execute( "foobar" ) );
        assertThat( incorrectUsage.getMessage(), containsString( "Available commands are: foo bar baz" ) );
    }

    @Test
    void testAdminUsage() throws Exception
    {
        CommandLocator commandLocator = mock( CommandLocator.class );
        List<AdminCommand.Provider> mockCommands = asList( mockCommand( "foo" ), mockCommand( "bar" ), mockCommand( "baz" ) );
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
    void showsArgumentsAndDescriptionForSpecifiedCommand() throws Exception
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
                            "  --database=<name>   Name of database. [default:" + GraphDatabaseSettings.DEFAULT_DATABASE_NAME + "]%n" ),
                    baos.toString() );
        }
    }

    private static AdminCommand.Provider mockCommand( String name )
    {
        AdminCommand.Provider commandProvider = mock( AdminCommand.Provider.class );
        when( commandProvider.name() ).thenReturn( name );
        when( commandProvider.commandSection() ).thenReturn( AdminCommandSection.general() );
        return commandProvider;
    }
}
