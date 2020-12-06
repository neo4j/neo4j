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
package org.neo4j.shell.commands;

import org.junit.Test;

import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.ShellParameterMap;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.Encryption;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.prettyprint.PrettyConfig;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.util.Versions.majorVersion;
import static org.neo4j.shell.util.Versions.minorVersion;

public class CypherShellProtocolIntegrationTest
{

    @Test
    public void shouldConnectWithBoltProtocol() throws Exception
    {
        CypherShell shell = new CypherShell( new StringLinePrinter(), new PrettyConfig( Format.PLAIN, true, 1000 ), false, new ShellParameterMap() );
        shell.connect( new ConnectionConfig( "bolt", "localhost", 7687, "neo4j", "neo", Encryption.DEFAULT, ABSENT_DB_NAME ) );
        assertTrue( shell.isConnected() );
    }

    @Test
    public void shouldConnectWithNeo4jProtocol() throws Exception
    {
        CypherShell shell = new CypherShell( new StringLinePrinter(), new PrettyConfig( Format.PLAIN, true, 1000 ), false, new ShellParameterMap() );
        // This should work even on older databases without the neo4j protocol, by falling back to bolt
        shell.connect( new ConnectionConfig( "neo4j", "localhost", 7687, "neo4j", "neo", Encryption.DEFAULT, ABSENT_DB_NAME ) );
        assertTrue( shell.isConnected() );
    }

    @Test
    public void shouldConnectWithBoltSSCProtocol() throws Exception
    {
        CypherShell shell = new CypherShell( new StringLinePrinter(), new PrettyConfig( Format.PLAIN, true, 1000 ), false, new ShellParameterMap() );
        // Given 3.X series where X > 1, where SSC are the default. Hard to test in 4.0 sadly.
        onlyIn3_2to3_6( shell );
        shell.connect( new ConnectionConfig( "bolt+ssc", "localhost", 7687, "neo4j", "neo", Encryption.DEFAULT, ABSENT_DB_NAME ) );
        assertTrue( shell.isConnected() );
    }

    @Test
    public void shouldConnectWithNeo4jSSCProtocol() throws Exception
    {
        CypherShell shell = new CypherShell( new StringLinePrinter(), new PrettyConfig( Format.PLAIN, true, 1000 ), false, new ShellParameterMap() );
        // Given 3.X series where X > 1, where SSC are the default. Hard to test in 4.0 sadly.
        onlyIn3_2to3_6( shell );
        // This should work by falling back to bolt+ssc
        shell.connect( new ConnectionConfig( "neo4j+ssc", "localhost", 7687, "neo4j", "neo", Encryption.DEFAULT, ABSENT_DB_NAME ) );
        assertTrue( shell.isConnected() );
    }

    // Here should be tests for "neo4j+s" and "bolt+s", but we don't have the infrastructure for those.

    private void onlyIn3_2to3_6( CypherShell shell ) throws Exception
    {
        // Default connection settings
        shell.connect( new ConnectionConfig( "bolt", "localhost", 7687, "neo4j", "neo", Encryption.DEFAULT, ABSENT_DB_NAME ) );
        assumeTrue( majorVersion( shell.getServerVersion() ) == 3 );
        assumeTrue( minorVersion( shell.getServerVersion() ) > 1 );
        shell.disconnect();
    }
}
