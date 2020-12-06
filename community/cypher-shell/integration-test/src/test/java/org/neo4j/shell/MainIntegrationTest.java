/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.shell;

import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;

import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.log.AnsiLogger;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.prettyprint.PrettyConfig;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MainIntegrationTest
{

    private String inputString = String.format( "neo4j%nneo%n" );
    private ByteArrayOutputStream baos;
    private ConnectionConfig connectionConfig;
    private CypherShell shell;
    private Main main;
    private PrintStream printStream;
    private InputStream inputStream;

    @Before
    public void setup()
    {
        // given
        inputStream = new ByteArrayInputStream( inputString.getBytes() );

        baos = new ByteArrayOutputStream();
        printStream = new PrintStream( baos );

        main = new Main( inputStream, printStream );

        CliArgs cliArgs = new CliArgs();
        cliArgs.setUsername( "", "" );
        cliArgs.setPassword( "", "" );

        Logger logger = new AnsiLogger( cliArgs.getDebugMode() );
        PrettyConfig prettyConfig = new PrettyConfig( cliArgs );
        connectionConfig = new ConnectionConfig(
                cliArgs.getScheme(),
                cliArgs.getHost(),
                cliArgs.getPort(),
                cliArgs.getUsername(),
                cliArgs.getPassword(),
                cliArgs.getEncryption() );

        shell = new CypherShell( logger, prettyConfig );
    }

    @Test
    public void promptsOnWrongAuthenticationIfInteractive() throws Exception
    {
        // when
        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        main.connectMaybeInteractively( shell, connectionConfig, true, true );

        // then
        // should be connected
        assertTrue( shell.isConnected() );
        // should have prompted and set the username and password
        assertEquals( "neo4j", connectionConfig.username() );
        assertEquals( "neo", connectionConfig.password() );

        String out = baos.toString();
        assertEquals( String.format( "username: neo4j%npassword: ***%n" ), out );
    }

    @Test
    public void doesNotPromptToStdOutOnWrongAuthenticationIfOutputRedirected() throws Exception
    {
        // when
        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        // Redirect System.in and System.out
        InputStream stdIn = System.in;
        PrintStream stdOut = System.out;
        System.setIn( inputStream );
        System.setOut( printStream );

        // Create a Main with the standard in and out
        try
        {
            Main realMain = new Main();
            realMain.connectMaybeInteractively( shell, connectionConfig, true, false );

            // then
            // should be connected
            assertTrue( shell.isConnected() );
            // should have prompted silently and set the username and password
            assertEquals( "neo4j", connectionConfig.username() );
            assertEquals( "neo", connectionConfig.password() );

            String out = baos.toString();
            assertEquals( "", out );
        }
        finally
        {
            // Restore in and out
            System.setIn( stdIn );
            System.setOut( stdOut );
        }
    }
}
