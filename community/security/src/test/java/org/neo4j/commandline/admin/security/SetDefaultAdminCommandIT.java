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
package org.neo4j.commandline.admin.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.server.security.auth.FileUserRepository;
import picocli.CommandLine;

class SetDefaultAdminCommandIT {
    private final FileSystemAbstraction fileSystem = new EphemeralFileSystemAbstraction();
    private Path confDir;
    private Path homeDir;
    private PrintStream out;
    private PrintStream err;

    @BeforeEach
    void setup() {
        Path graphDir = Path.of(GraphDatabaseSettings.DEFAULT_DATABASE_NAME);
        confDir = graphDir.resolveSibling("conf");
        homeDir = graphDir.resolveSibling("home");
        out = mock(PrintStream.class);
        err = mock(PrintStream.class);
    }

    @Test
    void shouldSetDefaultAdmin() throws Throwable {
        execute("jane");
        assertAdminIniFile("jane");

        verify(out).println("default admin user set to 'jane'");
    }

    @Test
    void shouldOverwrite() throws Throwable {
        execute("jane");
        assertAdminIniFile("jane");
        execute("janette");
        assertAdminIniFile("janette");

        verify(out).println("default admin user set to 'jane'");
        verify(out).println("default admin user set to 'janette'");
    }

    private void assertAdminIniFile(String username) throws Throwable {
        Path adminIniFile = homeDir.resolve("data").resolve("dbms").resolve(SetDefaultAdminCommand.ADMIN_INI);
        Assertions.assertTrue(fileSystem.fileExists(adminIniFile));
        FileUserRepository userRepository = new FileUserRepository(
                fileSystem, adminIniFile, NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE);
        userRepository.start();
        assertThat(userRepository.getAllUsernames()).contains(username);
        userRepository.stop();
        userRepository.shutdown();
    }

    private void execute(String username) {
        final var command = new SetDefaultAdminCommand(new ExecutionContext(homeDir, confDir, out, err, fileSystem));
        CommandLine.populateCommand(command, username);
        command.execute();
    }
}
