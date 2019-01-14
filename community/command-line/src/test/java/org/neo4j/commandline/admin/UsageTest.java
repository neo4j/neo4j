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
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import org.neo4j.commandline.arguments.Arguments;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UsageTest
{
    @Mock
    private Consumer<String> out;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks( this );
    }

    @Test
    public void shouldPrintUsageForACommand()
    {
        // given
        AdminCommand.Provider commandProvider = mockCommand( "bam", "A summary", AdminCommandSection.general() );
        AdminCommand.Provider[] commands = new AdminCommand.Provider[]{commandProvider};
        final Usage usage = new Usage( "neo4j-admin", new CannedLocator( commands ) );

        // when
        usage.printUsageForCommand( commandProvider, out );

        // then
        InOrder ordered = inOrder( out );
        ordered.verify( out ).accept( "usage: neo4j-admin bam " );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "environment variables:" );
        ordered.verify( out ).accept( "    NEO4J_CONF    Path to directory which contains neo4j.conf." );
        ordered.verify( out ).accept( "    NEO4J_DEBUG   Set to anything to enable debug output." );
        ordered.verify( out ).accept( "    NEO4J_HOME    Neo4j home directory." );
        ordered.verify( out ).accept( "    HEAP_SIZE     Set JVM maximum heap size during command execution." );
        ordered.verify( out ).accept( "                  Takes a number and a unit, for example 512m." );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "description" );
    }

    @Test
    public void shouldPrintUsageWithConfiguration()
    {
        AdminCommand.Provider[] commands =
                new AdminCommand.Provider[]{mockCommand( "bam", "A summary", AdminCommandSection.general() )};
        final Usage usage = new Usage( "neo4j-admin", new CannedLocator( commands ) );
        usage.print( out );

        InOrder ordered = inOrder( out );
        ordered.verify( out ).accept( "usage: neo4j-admin <command>" );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "Manage your Neo4j instance." );
        ordered.verify( out ).accept( "" );

        ordered.verify( out ).accept( "environment variables:" );
        ordered.verify( out ).accept( "    NEO4J_CONF    Path to directory which contains neo4j.conf." );
        ordered.verify( out ).accept( "    NEO4J_DEBUG   Set to anything to enable debug output." );
        ordered.verify( out ).accept( "    NEO4J_HOME    Neo4j home directory." );
        ordered.verify( out ).accept( "    HEAP_SIZE     Set JVM maximum heap size during command execution." );
        ordered.verify( out ).accept( "                  Takes a number and a unit, for example 512m." );
        ordered.verify( out ).accept( "" );

        ordered.verify( out ).accept( "available commands:" );
        ordered.verify( out ).accept( "General" );
        ordered.verify( out ).accept( "    bam" );
        ordered.verify( out ).accept( "        A summary" );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "Use neo4j-admin help <command> for more details." );
        ordered.verifyNoMoreInteractions();
    }

    @Test
    public void commandsUnderSameAdminCommandSectionPrintableSectionShouldAppearTogether()
    {
        AdminCommand.Provider[] commands = new AdminCommand.Provider[]{
                mockCommand( "first-command", "first-command", AdminCommandSection.general() ),
                mockCommand( "second-command", "second-command", new TestGeneralSection() )};
        final Usage usage = new Usage( "neo4j-admin", new CannedLocator( commands ) );
        usage.print( out );

        InOrder ordered = inOrder( out );
        ordered.verify( out ).accept( "usage: neo4j-admin <command>" );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "Manage your Neo4j instance." );
        ordered.verify( out ).accept( "" );

        ordered.verify( out ).accept( "environment variables:" );
        ordered.verify( out ).accept( "    NEO4J_CONF    Path to directory which contains neo4j.conf." );
        ordered.verify( out ).accept( "    NEO4J_DEBUG   Set to anything to enable debug output." );
        ordered.verify( out ).accept( "    NEO4J_HOME    Neo4j home directory." );
        ordered.verify( out ).accept( "    HEAP_SIZE     Set JVM maximum heap size during command execution." );
        ordered.verify( out ).accept( "                  Takes a number and a unit, for example 512m." );
        ordered.verify( out ).accept( "" );

        ordered.verify( out ).accept( "available commands:" );
        ordered.verify( out ).accept( "General" );
        ordered.verify( out ).accept( "    first-command" );
        ordered.verify( out ).accept( "        first-command" );
        ordered.verify( out ).accept( "    second-command" );
        ordered.verify( out ).accept( "        second-command" );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "Use neo4j-admin help <command> for more details." );
        ordered.verifyNoMoreInteractions();
    }

    private static class TestGeneralSection extends AdminCommandSection
    {

        @Override
        @Nonnull
        public String printable()
        {
            return "General";
        }
    }

    private AdminCommand.Provider mockCommand( String name, String summary, AdminCommandSection section )
    {
        AdminCommand.Provider commandProvider = mock( AdminCommand.Provider.class );
        when( commandProvider.name() ).thenReturn( name );
        when( commandProvider.summary() ).thenReturn( summary );
        when( commandProvider.allArguments() ).thenReturn( Arguments.NO_ARGS );
        when( commandProvider.possibleArguments() ).thenReturn( Collections.singletonList( Arguments.NO_ARGS ) );
        when( commandProvider.description() ).thenReturn( "description" );
        when( commandProvider.commandSection() ).thenReturn( section );
        return commandProvider;
    }
}
