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

import static java.util.Arrays.asList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class AdminToolTest
{
    @Test
    public void shouldExecuteTheCommand()
    {
        RecordingCommand command = new RecordingCommand();
        new AdminTool( new CannedLocator( command ), new NullOutput(), "", false ).execute( null, null, "foo" );
        assertThat( command.executed, is( true ) );
    }

    @Test
    public void shouldAddTheHelpCommand()
    {
        Output output = mock( Output.class );
        new AdminTool( new NullCommandLocator(), output, "", false ).execute( null, null, "help" );
        verify( output ).line( "neo4j-admin help" );
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
                public AdminCommand create( Path homeDir, Path configDir )
                {
                    return RecordingCommand.this;
                }
            };
        }
    }

    private static class NullOutput implements Output
    {
        @Override
        public void line( String text )
        {
        }
    }

    private static class CannedLocator implements CommandLocator
    {
        private final RecordingCommand command;

        public CannedLocator( RecordingCommand command )
        {
            this.command = command;
        }

        @Override
        public AdminCommand.Provider apply( String s )
        {
            return command.provider();
        }

        @Override
        public Iterable<AdminCommand.Provider> get()
        {
            return asList( command.provider() );
        }
    }

    private static class NullCommandLocator implements CommandLocator
    {
        @Override
        public AdminCommand.Provider apply( String s )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }

        @Override
        public Iterable<AdminCommand.Provider> get()
        {
            return Iterables.empty();
        }
    }
}
