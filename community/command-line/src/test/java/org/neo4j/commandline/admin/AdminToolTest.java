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

import org.junit.Test;

import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.helpers.collection.Iterables;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AdminToolTest
{
    @Test
    public void shouldExecuteTheCommand() throws CommandFailed, IncorrectUsage
    {
        AdminCommand command = mock( AdminCommand.class );
        new AdminTool( cannedCommand( "command", command ), new NullOutsideWorld(), false )
                .execute( null, null, "command", "the", "other", "args" );
        verify( command ).execute( new String[]{"the", "other", "args"} );
    }

    @Test
    public void shouldExit0WhenEverythingWorks()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new CannedLocator( new NullCommandProvider() ), outsideWorld, false )
                .execute( null, null, "null" );
        verify( outsideWorld ).exit( 0 );
    }

    @Test
    public void shouldAddTheHelpCommandToThoseProvidedByTheLocator()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new NullCommandLocator(), outsideWorld, false ).execute( null, null, "help" );
        verify( outsideWorld ).stdOutLine( "neo4j-admin help" );
    }

    @Test
    public void shouldProvideFeedbackWhenNoCommandIsProvided()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new NullCommandLocator(), outsideWorld, false ).execute( null, null );
        verify( outsideWorld ).stdErrLine( "you must provide a command" );
        verify( outsideWorld ).stdErrLine( "Usage:" );
        verify( outsideWorld ).exit( 1 );
    }

    @Test
    public void shouldProvideFeedbackIfTheCommandThrowsARuntimeException()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        AdminCommand command = args ->
        {
            throw new RuntimeException( "the-exception-message" );
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld ).stdErrLine( "unexpected error: the-exception-message" );
        verify( outsideWorld ).exit( 1 );
    }

    @Test
    public void shouldPrintTheStacktraceWhenTheCommandThrowsARuntimeExceptionIfTheDebugFlagIsSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        RuntimeException exception = new RuntimeException( "" );
        AdminCommand command = args ->
        {
            throw exception;
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, true )
                .execute( null, null, "exception" );
        verify( outsideWorld ).printStacktrace( exception );
    }

    @Test
    public void shouldNotPrintTheStacktraceWhenTheCommandThrowsARuntimeExceptionIfTheDebugFlagIsNotSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        RuntimeException exception = new RuntimeException( "" );
        AdminCommand command = args ->
        {
            throw exception;
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld, never() ).printStacktrace( exception );
    }

    @Test
    public void shouldProvideFeedbackIfTheCommandFails()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        AdminCommand command = args ->
        {
            throw new CommandFailed( "the-failure-message" );
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld ).stdErrLine( "command failed: the-failure-message" );
        verify( outsideWorld ).exit( 1 );
    }

    @Test
    public void shouldPrintTheStacktraceWhenTheCommandFailsIfTheDebugFlagIsSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CommandFailed exception = new CommandFailed( "" );
        AdminCommand command = args ->
        {
            throw exception;
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, true )
                .execute( null, null, "exception" );
        verify( outsideWorld ).printStacktrace( exception );
    }

    @Test
    public void shouldNotPrintTheStacktraceWhenTheCommandFailsIfTheDebugFlagIsNotSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        CommandFailed exception = new CommandFailed( "" );
        AdminCommand command = args ->
        {
            throw exception;
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld, never() ).printStacktrace( exception );
    }

    @Test
    public void shouldProvideFeedbackIfTheCommandReportsAUsageProblem()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        AdminCommand command = args ->
        {
            throw new IncorrectUsage( "the-usage-message" );
        };
        new AdminTool( cannedCommand( "exception", command ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld ).stdErrLine( "neo4j-admin exception" );
        verify( outsideWorld ).stdErrLine( "the-usage-message" );
        verify( outsideWorld ).exit( 1 );
    }

    private CannedLocator cannedCommand( final String name, AdminCommand command )
    {
        return new CannedLocator( new AdminCommand.Provider( name )
        {
            @Override
            public Optional<String> arguments()
            {
                return Optional.empty();
            }

            @Override
            public String description()
            {
                return "";
            }

            @Override
            public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
            {
                return command;
            }
        } );
    }

    private static class NullCommandLocator implements CommandLocator
    {
        @Override
        public AdminCommand.Provider findProvider( String s )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public Iterable<AdminCommand.Provider> getAllProviders()
        {
            return Iterables.empty();
        }
    }

    private class NullCommandProvider extends AdminCommand.Provider
    {
        protected NullCommandProvider()
        {
            super( "null" );
        }

        @Override
        public Optional<String> arguments()
        {
            return Optional.empty();
        }

        @Override
        public String description()
        {
            return "";
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            return args ->
            {
            };
        }
    }
}
