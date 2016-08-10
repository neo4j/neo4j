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
import java.util.function.Consumer;

import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.inOrder;

public class UsageTest
{
    @Mock
    private Consumer<String> out;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldPrintUsageForAllCommands()
    {
        AdminCommand.Provider[] commands = new AdminCommand.Provider[]
                {
                        new StubProvider( "restore",
                                Optional.of( "---from <backup-directory> --database=<database-name> [--force]" ),
                                "Restores a database backed up using the neo4j-backup tool." ),
                        new StubProvider( "bam", Optional.empty(), "Some description" )
                };
        final Usage usage = new Usage( "neo4j-admin", new CannedLocator( commands ) );
        usage.print( out );

        InOrder ordered = inOrder( out );
        ordered.verify( out ).accept( "Usage:" );
        ordered.verify( out ).accept( "" );
        ordered.verify( out )
                .accept( "neo4j-admin restore ---from <backup-directory> --database=<database-name> [--force]" );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "    Restores a database backed up using the neo4j-backup tool." );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "neo4j-admin bam" );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "    Some description" );
        ordered.verify( out ).accept( "" );
        ordered.verifyNoMoreInteractions();
    }

    private static class StubProvider extends AdminCommand.Provider
    {
        private final Optional<String> arguments;
        private final String description;

        public StubProvider( String name, Optional<String> arguments, String description )
        {
            super( name );
            this.arguments = arguments;
            this.description = description;
        }

        @Override
        public Optional<String> arguments()
        {
            return arguments;
        }

        @Override
        public String description()
        {
            return description;
        }

        @Override
        public AdminCommand create( Path homeDir, Path configDir, OutsideWorld outsideWorld )
        {
            throw new UnsupportedOperationException( "not implemented" );
        }
    }
}
