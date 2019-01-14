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
package org.neo4j.server;

import java.io.PrintStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.commandline.Util.neo4jVersion;

public class CommunityEntryPointTest
{
    private PrintStream realSystemOut;
    private PrintStream fakeSystemOut;

    @Before
    public void setup()
    {
        realSystemOut = System.out;
        fakeSystemOut = mock( PrintStream.class );
        System.setOut( fakeSystemOut );
    }

    @After
    public void teardown()
    {
        System.setOut( realSystemOut );
    }

    @Test
    public void mainPrintsVersion()
    {
        // when
        CommunityEntryPoint.main( new String[]{ "--version" } );

        // then
        verify( fakeSystemOut ).println( "neo4j " + neo4jVersion() );
        verifyNoMoreInteractions( fakeSystemOut );
    }
}
