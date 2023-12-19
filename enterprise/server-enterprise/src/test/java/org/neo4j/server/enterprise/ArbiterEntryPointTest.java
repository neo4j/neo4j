/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.enterprise;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.PrintStream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.commandline.Util.neo4jVersion;

public class ArbiterEntryPointTest
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
        ArbiterEntryPoint.main( new String[]{ "--version" } );

        // then
        verify( fakeSystemOut ).println( "neo4j " + neo4jVersion() );
        verifyNoMoreInteractions( fakeSystemOut );
    }
}
