/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.commands;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.shell.test.Util.testConnectionConfig;
import static org.neo4j.shell.util.Versions.majorVersion;
import static org.neo4j.shell.util.Versions.minorVersion;

import org.junit.jupiter.api.Test;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.StringLinePrinter;
import org.neo4j.shell.cli.AccessMode;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.state.BoltStateHandler;

class CypherShellProtocolIntegrationTest {

    @Test
    void shouldConnectWithBoltProtocol() throws Exception {
        CypherShell shell = shell();
        try {
            shell.connect(testConnectionConfig("bolt://localhost:7687").withUsernameAndPassword("neo4j", "neo"));
            assertTrue(shell.isConnected());
        } finally {
            shell.disconnect();
        }
    }

    @Test
    void shouldConnectWithNeo4jProtocol() throws Exception {
        CypherShell shell = shell();
        try {
            // This should work even on older databases without the neo4j protocol, by falling back to bolt
            shell.connect(testConnectionConfig("neo4j://localhost:7687").withUsernameAndPassword("neo4j", "neo"));
            assertTrue(shell.isConnected());
        } finally {
            shell.disconnect();
        }
    }

    @Test
    void shouldConnectWithBoltSSCProtocol() throws Exception {
        CypherShell shell = shell();
        try {
            // Given 3.X series where X > 1, where SSC are the default. Hard to test in 4.0 sadly.
            onlyIn3_2to3_6(shell);
            shell.connect(testConnectionConfig("bolt+ssc://localhost:7687").withUsernameAndPassword("neo4j", "neo"));
            assertTrue(shell.isConnected());
        } finally {
            shell.disconnect();
        }
    }

    @Test
    void shouldConnectWithNeo4jSSCProtocol() throws Exception {
        CypherShell shell = shell();
        try {
            // Given 3.X series where X > 1, where SSC are the default. Hard to test in 4.0 sadly.
            onlyIn3_2to3_6(shell);
            // This should work by falling back to bolt+ssc
            shell.connect(testConnectionConfig("neo4j+ssc://localhost:7687").withUsernameAndPassword("neo4j", "neo"));
            assertTrue(shell.isConnected());
        } finally {
            shell.disconnect();
        }
    }

    private CypherShell shell() {
        var boltHandler = new BoltStateHandler(true, AccessMode.WRITE);
        var printer = new PrettyPrinter(new PrettyConfig(Format.PLAIN, true, 1000, false));
        var parameters = mock(ParameterService.class);
        return new CypherShell(new StringLinePrinter(), boltHandler, printer, parameters);
    }

    // Here should be tests for "neo4j+s" and "bolt+s", but we don't have the infrastructure for those.

    private static void onlyIn3_2to3_6(CypherShell shell) throws Exception {
        // Default connection settings
        shell.connect(testConnectionConfig("bolt://localhost:7687").withUsernameAndPassword("neo4j", "neo"));
        assumeTrue(majorVersion(shell.getServerVersion()) == 3);
        assumeTrue(minorVersion(shell.getServerVersion()) > 1);
        shell.disconnect();
    }
}
