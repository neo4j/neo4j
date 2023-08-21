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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.neo4j.server.security.auth.CommunitySecurityModule.getInitialUserRepositoryFile;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.server.security.auth.FileUserRepository;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import picocli.CommandLine;

@EphemeralTestDirectoryExtension
class SetDefaultAdminCommandTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private TestDirectory testDir;

    private SetDefaultAdminCommand command;
    private Path adminIniFile;

    @BeforeEach
    void setup() {
        command = new SetDefaultAdminCommand(new ExecutionContext(
                testDir.directory("home"),
                testDir.directory("conf"),
                mock(PrintStream.class),
                mock(PrintStream.class),
                fileSystem));
        final Config config = command.loadNeo4jConfig();
        adminIniFile = getInitialUserRepositoryFile(config).resolveSibling("admin.ini");
    }

    @Test
    void printUsageHelp() {
        final var baos = new ByteArrayOutputStream();
        try (var out = new PrintStream(baos)) {
            CommandLine.usage(command, new PrintStream(out), CommandLine.Help.Ansi.OFF);
        }
        assertThat(baos.toString().trim())
                .isEqualToIgnoringNewLines(
                        """
                                USAGE

                                set-default-admin [-h] [--expand-commands] [--verbose]
                                                  [--additional-config=<file>] <username>

                                DESCRIPTION

                                Sets the default admin user.
                                This user will be granted the admin role on startup if the system has no roles.

                                PARAMETERS

                                      <username>

                                OPTIONS

                                      --additional-config=<file>
                                                          Configuration file with additional configuration.
                                      --expand-commands   Allow command expansion in config value evaluation.
                                  -h, --help              Show this help message and exit.
                                      --verbose           Enable verbose output.""");
    }

    @Test
    void shouldSetDefaultAdmin() throws Throwable {
        // Given
        assertFalse(fileSystem.fileExists(adminIniFile));

        // When
        CommandLine.populateCommand(command, "jake");

        command.execute();

        // Then
        assertAdminIniFile("jake");
    }

    @SuppressWarnings("SameParameterValue")
    private void assertAdminIniFile(String username) throws Throwable {
        assertTrue(fileSystem.fileExists(adminIniFile));
        FileUserRepository userRepository = new FileUserRepository(
                fileSystem, adminIniFile, NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE);
        userRepository.start();
        assertThat(userRepository.getAllUsernames()).contains(username);
    }
}
