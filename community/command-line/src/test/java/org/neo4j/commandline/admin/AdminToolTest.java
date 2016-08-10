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

import java.nio.file.Path;
import java.util.Optional;

import org.junit.Test;

import org.neo4j.helpers.collection.Iterables;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class AdminToolTest
{
    @Test
    public void shouldExecuteTheCommand()
    {
        RecordingCommand command = new RecordingCommand();
        AdminCommand.Provider provider = command.provider();
        AdminTool tool = new AdminTool( new CannedLocator( provider ), new NullOutsideWorld(), false );
        tool.execute( null, null, provider.name() );
        assertThat( command.executed, is( true ) );
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
    public void shouldPrintUsageWhenNoCommandIsProvided()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new NullCommandLocator(), outsideWorld, false ).execute( null, null );
        verify( outsideWorld ).stdErrLine( "Usage:" );
    }

    @Test
    public void shouldPrintASpecificMessageWhenNoCommandIsProvided()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new NullCommandLocator(), outsideWorld, false ).execute( null, null );
        verify( outsideWorld ).stdErrLine( "you must provide a command" );
    }

    @Test
    public void shouldExit1WhenNoCommandIsProvided()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new NullCommandLocator(), outsideWorld, false ).execute( null, null );
        verify( outsideWorld ).exit( 1 );
    }

    @Test
    public void shouldNotThrowAnExceptionEvenIfTheCommandDoesSo()
    {
        new AdminTool( new CannedLocator( new ExceptionThrowingCommandProvider() ), new NullOutsideWorld(), false )
                .execute( null, null, "exception" );
    }

    @Test
    public void shouldExit1IfTheCommandThrowsAnException()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        new AdminTool( new CannedLocator( new ExceptionThrowingCommandProvider() ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld ).exit( 1 );
    }

    @Test
    public void shouldPrintTheStacktraceWhenTheCommandThrowsAnExceptionIfTheDebugFlagIsSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        RuntimeException exception = new RuntimeException();
        new AdminTool( new CannedLocator( new ExceptionThrowingCommandProvider( exception ) ), outsideWorld, true )
                .execute( null, null, "exception" );
        verify( outsideWorld ).printStacktrace( exception );
    }

    @Test
    public void shouldNotPrintTheStacktraceWhenTheCommandThrowsAnExceptionIfTheDebugFlagIsNotSet()
    {
        OutsideWorld outsideWorld = mock( OutsideWorld.class );
        RuntimeException exception = new RuntimeException();
        new AdminTool( new CannedLocator( new ExceptionThrowingCommandProvider( exception ) ), outsideWorld, false )
                .execute( null, null, "exception" );
        verify( outsideWorld, never() ).printStacktrace( exception );
    }

    private static class RecordingCommand implements AdminCommand
    {
        public boolean executed;

        @Override
        public void execute( String[] args )
        {
            executed = true;
        }

        public Provider provider()
        {
            return new Provider( "recording" )
            {
                @Override
                public Optional<String> arguments()
                {
                    throw new UnsupportedOperationException( "not implemented" );
                }

                @Override
                public String description()
                {
                    throw new UnsupportedOperationException( "not implemented" );
                }

                @Override
                public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
                {
                    return RecordingCommand.this;
                }
            };
        }
    }

    private static class NullOutsideWorld implements OutsideWorld
    {
        @Override
        public void stdOutLine( String text )
        {
        }

        @Override
        public void stdErrLine( String text )
        {
        }

        @Override
        public void exit( int status )
        {
        }

        @Override
        public void printStacktrace( Exception exception )
        {
        }
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

    private class ExceptionThrowingCommandProvider extends AdminCommand.Provider
    {
        private RuntimeException exception;

        protected ExceptionThrowingCommandProvider()
        {
            this( new RuntimeException() );
        }

        public ExceptionThrowingCommandProvider( RuntimeException exception )
        {
            super( "exception" );
            this.exception = exception;
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
                throw exception;
            };
        }
    }
}
