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

import java.util.HashMap;
import java.util.List;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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

        List<AdminCommand.Provider> providers =
                asList( mockCommand( "restore", "Restore" ), mockCommand( "bam", "A summary" ),
                        mockCommand( "zzzz-last-one", "Another summary" ) );
        generalSection.printAllCommandsUnderSection( out, providers );

        InOrder ordered = inOrder( out );
        ordered.verify( out ).accept( "" );
        ordered.verify( out ).accept( "General" );
        ordered.verify( out ).accept( "    bam" );
        ordered.verify( out ).accept( "        A summary" );
        ordered.verify( out ).accept( "    restore" );
        ordered.verify( out ).accept( "        Restore" );
        ordered.verify( out ).accept( "    zzzz-last-one" );
        ordered.verify( out ).accept( "        Another summary" );
        ordered.verifyNoMoreInteractions();
    }

    @Test
    public void equalsUsingReflection()
    {
        assertTrue( AdminCommandSection.general().equals( new TestGeneralSection() ) );
        assertFalse( AdminCommandSection.general().equals( new TestAnotherGeneralSection() ) );
    }

    @Test
    public void hashCodeUsingReflection()
    {
        TestGeneralSection testGeneralSection = new TestGeneralSection();
        TestAnotherGeneralSection testAnotherGeneralSection = new TestAnotherGeneralSection();
        HashMap<AdminCommandSection,String> map = new HashMap<>();
        map.put( AdminCommandSection.general(), "General-Original" );
        map.put( testGeneralSection, "General-Test" );
        map.put( testAnotherGeneralSection, "General-AnotherTest" );

        assertEquals( 2, map.size() );
        assertEquals( "General-Test", map.get( AdminCommandSection.general() ) );
        assertEquals( "General-Test", map.get( testGeneralSection ) );
        assertEquals( "General-AnotherTest", map.get( testAnotherGeneralSection ) );
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

    private static class TestAnotherGeneralSection extends AdminCommandSection
    {

        @Override
        @Nonnull
        public String printable()
        {
            return "Another Section";
        }
    }

    private AdminCommand.Provider mockCommand( String name, String summary )
    {
        AdminCommand.Provider commandProvider = mock( AdminCommand.Provider.class );
        when( commandProvider.name() ).thenReturn( name );
        when( commandProvider.summary() ).thenReturn( summary );
        when( commandProvider.commandSection() ).thenReturn( AdminCommandSection.general() );
        return commandProvider;
    }
}
