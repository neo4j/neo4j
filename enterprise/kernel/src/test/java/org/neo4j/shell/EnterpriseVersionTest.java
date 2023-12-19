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
package org.neo4j.shell;

import org.junit.Rule;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

public class EnterpriseVersionTest
{
    @Rule
    public final TestDirectory dir = TestDirectory.testDirectory();

    @Test
    public void shouldPrintVersionAndExit()
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
    public void shouldReportEditionThroughDbInfoApp()
    {
        // given
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        CtrlCHandler ctrlCHandler = mock( CtrlCHandler.class );
        StartClient client = new StartClient( new PrintStream( out ), new PrintStream( err ) );

        // when
        client.start( new String[]{"-path", dir.absolutePath().getPath(),
                "-c", "dbinfo -g Configuration unsupported.dbms.edition"}, ctrlCHandler );

        // then
        assertEquals( 0, err.size() );
        assertThat( out.toString(), containsString( "\"unsupported.dbms.edition\": \"enterprise\"" ) );
    }
}
