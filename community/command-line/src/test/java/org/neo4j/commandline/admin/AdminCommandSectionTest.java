/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AdminCommandSectionTest
{
    @Mock
    private Consumer<String> out;

    @Before
    public void setUp()
    {
        MockitoAnnotations.initMocks( this );
    }

    @Test
    public void shouldPrintUsageForAllCommandsAlphabetically()
    {
        AdminCommandSection generalSection = AdminCommandSection.general();

        List<AdminCommand.Provider> providers = asList( mockCommand( generalSection, "restore", "Restore" ),
                mockCommand( generalSection, "bam", "A summary" ),
                mockCommand( generalSection, "zzzz-last-one", "Another summary" ) );
        generalSection.printAllCommandsUnderSection( out, providers );

        InOrder ordered = inOrder( out );
        ordered.verify( out ).accept( "General" );
        ordered.verify( out ).accept( "    bam" );
        ordered.verify( out ).accept( "        A summary" );
        ordered.verify( out ).accept( "    restore" );
        ordered.verify( out ).accept( "        Restore" );
        ordered.verify( out ).accept( "    zzzz-last-one" );
        ordered.verify( out ).accept( "        Another summary" );
        ordered.verifyNoMoreInteractions();
    }

    private AdminCommand.Provider mockCommand( AdminCommandSection section, String name, String summary )
    {
            AdminCommand.Provider commandProvider = mock( AdminCommand.Provider.class );
            when( commandProvider.name() ).thenReturn( name );
            when( commandProvider.summary() ).thenReturn( summary );
            when( commandProvider.commandSection() ).thenReturn( AdminCommandSection.general() );
            return commandProvider;
    }
}
