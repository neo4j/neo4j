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

import static org.neo4j.shell.test.Util.testConnectionConfig;
import static org.neo4j.shell.util.Versions.version;

import org.neo4j.shell.CypherShell;
import org.neo4j.shell.exception.CommandException;

abstract class CypherShellIntegrationTest {
    CypherShell shell;

    void connect(String password) throws CommandException {
        shell.connect(testConnectionConfig("bolt://localhost:7687").withUsernameAndPassword("neo4j", password));
    }

    boolean runningAtLeast(String version) {
        return version(version).compareTo(version(shell.getServerVersion())) <= 0;
    }
}
