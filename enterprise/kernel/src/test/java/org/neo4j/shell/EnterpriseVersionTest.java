/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.shell;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.test.TargetDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.neo4j.test.TargetDirectory.testDirForTest;

public class EnterpriseVersionTest
{
    @Rule
    public final TargetDirectory.TestDirectory dir = testDirForTest( EnterpriseVersionTest.class );

    @Test
    public void shouldPrintVersionAndExit() throws Exception
    {
        // given
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CtrlCHandler ctrlCHandler = mock( CtrlCHandler.class );
        StartClient client = new StartClient( new PrintStream( out ), new PrintStream( err ) );

        // when
        client.start( new String[]{"-version"}, ctrlCHandler );

        // then
        assertEquals( 0, err.size() );
        String version = out.toString();
        assertThat( version, startsWith( "Neo4j Enterprise, version " ) );
    }

    @Test
    public void shouldReportEditionThroughDbInfoApp() throws Exception
    {
        // given
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CtrlCHandler ctrlCHandler = mock( CtrlCHandler.class );
        StartClient client = new StartClient( new PrintStream( out ), new PrintStream( err ) );

        // when
        client.start( new String[]{"-path", dir.absolutePath(),
                "-c", "dbinfo -g Configuration edition"}, ctrlCHandler );

        // then
        assertEquals( 0, err.size() );
        assertThat( out.toString(), containsString( "\"edition\": \"Enterprise\"" ) );
    }
}
